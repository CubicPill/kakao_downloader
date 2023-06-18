import datetime
import os
import shutil
import subprocess
import traceback
from enum import Enum
from queue import Empty, Queue
from threading import Lock, Thread

import ffmpeg
from PIL import Image

_MAGICK_BIN = shutil.which('magick')
_print_lock = Lock()
GIF_ALPHA_THRESHOLD = 1
WEBM_SIZE_KB_MAX = 256
WEBM_DURATION_SEC_MAX = 3


class OutputFormat(Enum):
    # this will also be the file extension
    GIF = 'gif'
    WEBM = 'webm'
    # MP4 = 'mp4'
    # APNG = 'png'


class Operation(Enum):
    SCALE = 'scale'
    REMOVE_ALPHA = 'remove_alpha'
    TO_GIF = 'to_gif'
    TO_WEBM = 'to_webm'


class ProcessTask:
    def __init__(self, sticker_id, in_img_path, in_audio_path, scale_px, operations,
                 result_output_path):
        self.sticker_id = sticker_id
        self.in_img = in_img_path
        self.in_audio = in_audio_path
        self.scale_px = scale_px
        self.operations = operations
        self.result_path = result_output_path


class KakaoProcessor(Thread):
    def __init__(self, queue, temp_dir, output_format):
        Thread.__init__(self)
        self.queue: Queue = queue
        self.temp_dir = temp_dir
        self.output_format = output_format
        self._current_sticker_id = None

    def run(self) -> None:

        # first, use imagemagick to split frames
        # use PIL to convert, ensure it's proper transparent png
        # then get frame duration, geometry, blending method, disposal method
        # finally use ffmpeg to convert to webm
        # then it can then be fed into regular processor

        while not self.queue.empty():
            try:
                task: ProcessTask = self.queue.get_nowait()
            except Empty:
                continue
            self._current_sticker_id = task.sticker_id
            result_temp_path = os.path.join(self.temp_dir, f'{task.sticker_id}.{self.output_format.value}.tmp')
            try:
                # frames need to be split first before processing
                frame_temp_dir = self.make_frame_temp_dir()
                durations = self.split_webp_frames(task.in_img, frame_temp_dir)

                for i, op in enumerate(task.operations):
                    if op == Operation.SCALE:
                        self.scale_frames(task.scale_px, frame_temp_dir)
                    elif op == Operation.REMOVE_ALPHA:
                        self.remove_alpha_frames(frame_temp_dir)
                    elif op == Operation.TO_GIF:
                        self.to_gif(durations, frame_temp_dir, result_temp_path)
                    elif op == Operation.TO_WEBM:
                        webm_interim = os.path.join(self.temp_dir, f'{self._current_sticker_id}.raw.webm')
                        self.to_webm(durations, frame_temp_dir, webm_interim)
                        self.cap_webm_duration_and_size(durations, webm_interim, frame_temp_dir, result_temp_path)

                shutil.copy(result_temp_path, task.result_path)

            # try:
            #     interim_file_path = os.path.join(self.temp_dir, f'conv_{uid}.webm')
            #     durations = self.to_webm(uid, in_file, interim_file_path)
            #     self.check_and_adjust_duration(uid, durations, interim_file_path, out_file)
            except ffmpeg.Error as e:
                with _print_lock:
                    print('Error occurred while processing', e, task.sticker_id)
                    print('------stdout------')
                    print(e.stdout.decode())
                    print('------end------')
                    print('------stderr------')
                    print(e.stderr.decode())
                    print('------end------')
                    traceback.print_exc()
                return
            except Exception as e:
                with _print_lock:
                    print('Error occurred while processing', e, task.sticker_id)
                    traceback.print_exc()
                return
            finally:
                self.queue.task_done()

    def make_frame_temp_dir(self):
        frame_working_dir_path = os.path.join(self.temp_dir, 'frames_' + self._current_sticker_id)
        if not os.path.isdir(frame_working_dir_path):
            os.mkdir(frame_working_dir_path)
        return frame_working_dir_path

    def _make_frame_file(self, durations, frame_working_dir_path):
        with open(os.path.join(frame_working_dir_path, 'frames.txt'), 'w') as f:
            for i, d in enumerate(durations):
                f.write(f"file 'frame-{i}.png'\n")
                f.write(f'duration {d}\n')
            # last frame need to be put twice, see: https://trac.ffmpeg.org/wiki/Slideshow
            f.write(f"file 'frame-{len(durations) - 1}.png'\n")
        return os.path.join(frame_working_dir_path, 'frames.txt')

    def scale_frames(self, scale_px, frame_dir):
        frame_scale_temp_dir = os.path.join(frame_dir, 'scale')
        if not os.path.isdir(frame_scale_temp_dir):
            os.mkdir(frame_scale_temp_dir)
        for frame_file in os.listdir(frame_dir):
            if frame_file.endswith('.png'):
                ffmpeg.input(os.path.join(frame_dir, frame_file)) \
                    .filter('scale', w=f'if(gt(iw,ih),{scale_px},-1)', h=f'if(gt(iw,ih),-1,{scale_px})') \
                    .output(os.path.join(frame_scale_temp_dir, frame_file)) \
                    .overwrite_output() \
                    .run(quiet=True)
        # remove original and move scaled to original location
        for frame_file in os.listdir(frame_scale_temp_dir):
            os.remove(os.path.join(frame_dir, frame_file))
            shutil.move(os.path.join(frame_scale_temp_dir, frame_file), os.path.join(frame_dir, frame_file))
        os.rmdir(frame_scale_temp_dir)

    def remove_alpha_frames(self, frame_dir):
        frame_scale_temp_dir = os.path.join(frame_dir, 'alpharm')
        if not os.path.isdir(frame_scale_temp_dir):
            os.mkdir(frame_scale_temp_dir)
        for frame_file in os.listdir(frame_dir):
            subprocess.call(
                    ['magick', 'convert', frame_file, '-background', 'white', '-alpha', 'remove', '-alpha', 'off',
                     os.path.join(frame_scale_temp_dir, frame_file)])
        # remove original and move scaled to original location
        for frame_file in os.listdir(frame_scale_temp_dir):
            os.remove(os.path.join(frame_dir, frame_file))
            shutil.move(os.path.join(frame_scale_temp_dir, frame_file), os.path.join(frame_dir, frame_file))
        os.rmdir(frame_scale_temp_dir)

    def split_webp_frames(self, in_file, frame_dir):
        # split frames using imagemagick, and reconstruct frames based on disposal/blending

        subprocess.call([_MAGICK_BIN, in_file, os.path.join(frame_dir, 'frame-%d.png')], shell=False)

        p = subprocess.Popen(
                [_MAGICK_BIN, 'identify', '-format', r'%T,%W,%H,%w,%h,%X,%Y,%[webp:mux-blend],%D|', in_file],
                stdin=None, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, shell=False)
        out, err = p.communicate()
        frame_data_str_output = out.decode().strip()[:-1]
        image_w, image_h = 0, 0
        frame_data = list()
        for fd in frame_data_str_output.split('|'):
            duration, cw, ch, w, h, x, y, blend_method, dispose_method = fd.split(',')
            duration = round(int(duration) / 100.0, 2)
            cw, ch, w, h, x, y = [int(i) for i in [cw, ch, w, h, x, y]]
            image_w, image_h = cw, ch
            frame_data.append((duration, (w, h, x, y), blend_method, dispose_method))

        # added for debugging purposes
        # if not os.path.isdir(os.path.join(frame_dir, 'raw')):
        #     os.mkdir(os.path.join(frame_dir, 'raw'))
        # for i in os.listdir(frame_dir):
        #     if i.startswith('frame-') and i.endswith('.png'):
        #         shutil.copy(os.path.join(frame_dir, i), os.path.join(frame_dir, 'raw'))

        self._process_blend_and_dispose(frame_data, frame_dir, image_h, image_w)

        durations = [f[0] for f in frame_data]
        return durations

    def _process_blend_and_dispose(self, frame_data, frame_dir, image_h, image_w):
        # background color should be transparent
        canvas = Image.new('RGBA', (image_w, image_h), (255, 255, 255, 0))
        for i, d in enumerate(frame_data):
            # since dispose_method applies to after displaying current frame,
            # for this frame we need data from last frame
            duration, (w, h, x, y), blend_method, _ = d
            if i == 0:
                dispose_method = 'None'
                _w, _h, _x, _y = 0, 0, 0, 0  # make linter happy
            else:
                _, (_w, _h, _x, _y), _, dispose_method = frame_data[i - 1]

            frame_file = os.path.join(frame_dir, f'frame-{i}.png')
            frame_image = Image.open(frame_file).convert('RGBA')

            if dispose_method == 'Background':
                # last frame to be disposed to background color (transparent)
                rect = Image.new('RGBA', (_w, _h), (255, 255, 255, 0))
                canvas.paste(rect, (_x, _y, _w + _x, _h + _y))

            # else do not dispose, do nothing
            if blend_method == 'AtopPreviousAlphaBlend':  # do not blend
                canvas.paste(frame_image, (x, y, w + x, h + y))
            else:  # alpha blending
                try:
                    canvas.paste(frame_image, (x, y, w + x, h + y), frame_image)
                except Exception as e:
                    print(frame_file, e)
                    raise Exception

            canvas.save(os.path.join(frame_dir, f'frame-{i}.png'))

    def to_gif(self, durations, frame_dir, out_file):
        frame_file_path = self._make_frame_file(durations, frame_dir)
        palette_stream = ffmpeg.input(frame_file_path, format='concat').filter('palettegen', reserve_transparent=1)
        ffmpeg.filter([ffmpeg.input(frame_file_path, format='concat'), palette_stream], 'paletteuse',
                      alpha_threshold=GIF_ALPHA_THRESHOLD) \
            .output(out_file, f='gif') \
            .overwrite_output() \
            .run(quiet=True)

    def to_webm(self, durations, frame_dir, out_file):
        # framerate is needed here since telegram ios client will use framerate as play speed
        # in fact, framerate in webm should be informative only
        # ffmpeg will use 25 by default, here according to telegram we use 30
        # so we set vsync=1 (cfr), let ffmpeg duplicate some frames to make ios happy
        # this will cause file size to increase a bit, but it should be OK
        # also 1/framerate seems to be the minimum unit of ffmpeg to encode frame duration
        # so shouldn't set it too small - which will cause too much error
        # https://bugs.telegram.org/c/14778
        frame_file_path = self._make_frame_file(durations, frame_dir)
        ffmpeg.input(frame_file_path, format='concat') \
            .filter('scale', w='if(gt(iw,ih),512,-1)', h='if(gt(iw,ih),-1,512)') \
            .output(out_file, r=30, vsync=1, f='webm') \
            .overwrite_output() \
            .run(quiet=True)

    def probe_duration(self, file):
        duration_str = ffmpeg.probe(file)['streams'][0]['tags']['DURATION']

        hms, us = duration_str.split('.')
        us = us[:6]
        duration_str = f'{hms}.{us}'
        duration_dt = datetime.datetime.strptime(duration_str, '%H:%M:%S.%f')
        duration_seconds = datetime.timedelta(seconds=duration_dt.second,
                                              microseconds=duration_dt.microsecond).total_seconds()
        return duration_seconds

    def cap_webm_duration_and_size(self, durations, in_webm, frame_dir, out_file):
        # probe, ensure it's max 3 seconds
        duration_seconds = self.probe_duration(in_webm)

        if duration_seconds > WEBM_DURATION_SEC_MAX:
            factor = duration_seconds / WEBM_DURATION_SEC_MAX
            while True:
                # loop to reduce frame duration until it's less than WEBM_DURATION_SEC_MAX seconds
                new_durations = [int(d / factor * 1000) / 1000 for d in durations]
                self.to_webm(new_durations, frame_dir, out_file)
                new_duration_seconds = self.probe_duration(file=out_file)
                if new_duration_seconds > 3:
                    factor = factor * 1.05
                else:
                    break
        else:  # just copy
            shutil.copyfile(in_webm, out_file)

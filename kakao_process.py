import datetime
import os
import shutil
import subprocess
import traceback
from enum import Enum
from queue import Empty, Queue
from threading import Lock, Thread

import ffmpeg

_MAGICK_BIN = shutil.which('magick')
_print_lock = Lock()
GIF_ALPHA_THRESHOLD = 1
WEBM_SIZE_KB_MAX = 256
WEBM_DURATION_SEC_MAX = 3
_counter_lock = Lock()
_task_completed_counter = 0


def get_counter_value():
    with _counter_lock:
        return _task_completed_counter


def increment_counter():
    with _counter_lock:
        global _task_completed_counter
        _task_completed_counter += 1


def reset_counter():
    with _counter_lock:
        global _task_completed_counter
        _task_completed_counter = 0


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

        while not self.queue.empty():
            try:
                task: ProcessTask = self.queue.get_nowait()
            except Empty:
                continue
            self._current_sticker_id = task.sticker_id
            try:
                # frames need to be split first before processing
                frame_temp_dir = self.make_frame_temp_dir()

                curr_in = task.in_img
                for i, op in enumerate(task.operations):
                    curr_out = os.path.join(self.temp_dir, f'{self._current_sticker_id}_interim_{i}.tmp')
                    if op == Operation.SCALE:
                        self.scale(curr_in, curr_out, task.scale_px)
                    elif op == Operation.REMOVE_ALPHA:
                        self.remove_alpha(curr_in, curr_out)
                    elif op == Operation.TO_GIF:
                        self.to_gif(curr_in, curr_out)
                    elif op == Operation.TO_WEBM:
                        frame_dir = self.make_frame_temp_dir()
                        durations = self.split_webp_frames(curr_in, frame_dir)
                        webm_uncapped = os.path.join(self.temp_dir, f'{self._current_sticker_id}.raw.webm')
                        self.to_webm(durations, frame_temp_dir, webm_uncapped)
                        self.cap_webm_duration_and_size(durations, webm_uncapped, frame_temp_dir, curr_out)
                    curr_in = curr_out
                shutil.copy(curr_in, task.result_path)

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
                increment_counter()

    def make_frame_temp_dir(self):
        frame_working_dir_path = os.path.join(self.temp_dir, 'frames_' + self._current_sticker_id)
        if not os.path.isdir(frame_working_dir_path):
            os.mkdir(frame_working_dir_path)
        return frame_working_dir_path

    def _make_frame_file(self, durations, frame_working_dir_path):
        with open(os.path.join(frame_working_dir_path, 'frames.txt'), 'w') as f:
            for i, d in enumerate(durations):
                f.write(f"file 'frame-{i:02d}.png'\n")
                f.write(f'duration {d}\n')
            # last frame need to be put twice, see: https://trac.ffmpeg.org/wiki/Slideshow
            # f.write(f"file 'frame-{len(durations) - 1}.png'\n")
        return os.path.join(frame_working_dir_path, 'frames.txt')

    def scale(self, in_file, out_file, scale_px):
        subprocess.call(
                ['magick', 'convert', 'WEBP:' + in_file, '-resize', f'{scale_px}x{scale_px}', 'WEBP:' + out_file])

    # def scale_frames(self, scale_px, frame_dir):
    #     frame_scale_temp_dir = os.path.join(frame_dir, 'scale')
    #     if not os.path.isdir(frame_scale_temp_dir):
    #         os.mkdir(frame_scale_temp_dir)
    #     for frame_file in os.listdir(frame_dir):
    #         if frame_file.endswith('.png'):
    #             ffmpeg.input(os.path.join(frame_dir, frame_file)) \
    #                 .filter('scale', w=f'if(gt(iw,ih),{scale_px},-1)', h=f'if(gt(iw,ih),-1,{scale_px})') \
    #                 .output(os.path.join(frame_scale_temp_dir, frame_file)) \
    #                 .overwrite_output() \
    #                 .run(quiet=True)
    #     # remove original and move scaled to original location
    #     for frame_file in os.listdir(frame_scale_temp_dir):
    #         os.remove(os.path.join(frame_dir, frame_file))
    #         shutil.move(os.path.join(frame_scale_temp_dir, frame_file), os.path.join(frame_dir, frame_file))
    #     os.rmdir(frame_scale_temp_dir)

    def remove_alpha(self, in_file, out_file):
        subprocess.call(['magick', 'convert', 'WEBP:' + in_file, '-background',
                         'white', '-alpha', 'remove', '-alpha', 'off',
                         'WEBP:' + out_file])

    # def remove_alpha_frames(self, frame_dir):
    #     frame_scale_temp_dir = os.path.join(frame_dir, 'alpharm')
    #     if not os.path.isdir(frame_scale_temp_dir):
    #         os.mkdir(frame_scale_temp_dir)
    #     for frame_file in os.listdir(frame_dir):
    #         subprocess.call(
    #                 ['magick', 'convert', frame_file, '-background', 'white', '-alpha', 'remove', '-alpha', 'off',
    #                  os.path.join(frame_scale_temp_dir, frame_file)])
    #     # remove original and move scaled to original location
    #     for frame_file in os.listdir(frame_scale_temp_dir):
    #         os.remove(os.path.join(frame_dir, frame_file))
    #         shutil.move(os.path.join(frame_scale_temp_dir, frame_file), os.path.join(frame_dir, frame_file))
    #     os.rmdir(frame_scale_temp_dir)

    def to_gif(self, in_file, out_file):
        # use imagemagick to convert webp to gif
        subprocess.call(
                ['magick', 'convert', 'WEBP:' + in_file, '-channel', 'A', '-threshold', '99%', 'GIF:' + out_file])
        subprocess.call(['magick', 'GIF:' + out_file, '-coalesce', 'GIF:' + out_file])

    def split_webp_frames(self, in_file, frame_dir):
        # split frames using imagemagick, and reconstruct frames based on disposal/blending
        if not os.path.isdir(frame_dir):
            os.mkdir(frame_dir)
        subprocess.call(
                [_MAGICK_BIN, 'WEBP:' + in_file, '-coalesce', os.path.join(frame_dir, 'frame-%02d.png')])

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

        # # added for debugging purposes
        # try:
        #     os.mkdir(os.path.join(frame_dir, 'raw'))
        # except:
        #     pass
        # for i in os.listdir(frame_dir):
        #     if i.startswith('frame-') and i.endswith('.png'):
        #         shutil.copy(os.path.join(frame_dir, i), os.path.join(frame_dir, 'raw'))

        durations = [f[0] for f in frame_data]
        return durations

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
            .output(out_file, r=30, fps_mode='cfr', f='webm') \
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
        # TODO even after optimization, webm file size may still exceed the limit. Lossy compression may be needed

        # probe duration, ensure it's max 3 seconds
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

        # see if file size is OK
        if os.path.getsize(out_file) > WEBM_SIZE_KB_MAX * 1024:
            # TODO optimize file size
            with _print_lock:
                print(f'WARNING: File size too large, {os.path.getsize(out_file) / 1024} KB')

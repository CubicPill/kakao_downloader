import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from queue import Queue

import requests
from tqdm import tqdm

from decrypt import data_xor
from kakao_process import KakaoProcessor, Operation, OutputFormat, ProcessTask

_HEADER_KAKAO = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 KAKAOTALK 10.2.4'}

_HEADER_CHROME_DEFAULT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
METADATA_URL_TEMPLATE = 'https://e.kakao.com/api/v1/items/t/{text_id}'
SHARE_LINK_TEMPLATE = 'https://emoticon.kakao.com/items/{share_link_id}'
_proxies = None
err_print = print
norm_print = print


def set_proxy(proxies):
    global _proxies
    _proxies = proxies


# for kakao, sticker types are: gif webp png


def get_num_pack_id_and_title(url):
    text_content = requests.get(url, headers=_HEADER_KAKAO, proxies=_proxies).text
    id_ptn = re.compile(r'kakaotalk://store/emoticon/(\d+)')
    title_ptn = re.compile(r'<title>(.+)</title>')
    num_pack_id = id_ptn.search(text_content).group(1)
    title = title_ptn.search(text_content).group(1)

    return num_pack_id, title


def get_sticker_text_id(url):
    resp = requests.get(url, headers=_HEADER_CHROME_DEFAULT, proxies=_proxies)
    # remove query string
    text_id = resp.url.split('/')[-1].split('?')[0]
    return text_id


def get_metadata(text_id):
    resp = requests.get(METADATA_URL_TEMPLATE.format(text_id=text_id), headers=_HEADER_KAKAO, proxies=_proxies)
    return resp.json()['result']


def get_sticker_archive(num_pack_id):
    archive_content = requests.get(f'https://item.kakaocdn.net/dw/{num_pack_id}.file_pack.zip', headers=_HEADER_KAKAO,
                                   proxies=_proxies)
    return archive_content.content


def main():
    arg_parser = argparse.ArgumentParser(description='Download stickers from Kakao store')
    arg_parser.add_argument('id_url', type=str, help='ID or share like of sticker set')

    arg_parser.add_argument('--proxy', type=str, help='HTTPS proxy, addr:port')
    arg_parser.add_argument('-y', action='store_true', help='Skip confirmation')
    arg_parser.add_argument('--redownload', action='store_true', help='Redownload stickers even if they exist')
    arg_parser.add_argument('--no-subdir', action='store_true',
                            help='Do not create subdirectory for different output formats')

    arg_parser.add_argument('--show', action='store_true', help='Open the download/output directory after download')
    arg_parser.add_argument('-q', '--quiet', action='store_true', help='Do not print information and progress bar')

    arg_parser.add_argument('--output-fmt', type=str, help='Output format', default='none',
                            choices=['none', 'gif', 'webm'], )

    arg_parser.add_argument('-t', '--threads', type=int, help='Thread number of processor, default 8', default=8)

    args = arg_parser.parse_args()

    sticker_data_root_dir = os.path.join(os.getcwd(), 'sticker_dl')
    if not os.path.exists(sticker_data_root_dir):
        os.mkdir(sticker_data_root_dir)

    default_sticker_output_root_dir = os.path.join(os.getcwd(), 'sticker_out')
    if not os.path.exists(default_sticker_output_root_dir):
        os.mkdir(default_sticker_output_root_dir)

    proxies = {}
    if args.proxy:
        proxies['https'] = args.proxy
        set_proxy(proxies)

    num_process_threads = args.threads

    id_url = args.id_url.strip()
    output_fmt = args.output_fmt
    skip_confirmation = args.y
    no_sub_dir = args.no_subdir
    open_folder = args.show
    quiet = args.quiet

    global norm_print
    if quiet:
        norm_print = lambda *args, **kwargs: None
        skip_confirmation = True

    if 'http' not in id_url:
        num_pack_id = id_url
        raise NotImplementedError('ID input is not supported yet')
    else:
        # input is url
        # extract pack id from url
        assert id_url.startswith('https://emoticon.kakao.com/items'), 'Invalid URL'
        share_link_id = id_url.split('/')[-1].split('?')[0]

    # from here, pack_id should be ready. Check if the sticker set has already been downloaded
    for d in os.listdir(sticker_data_root_dir):
        if os.path.isdir(os.path.join(sticker_data_root_dir, d)):
            # open metadata file to see if share_link_id match
            if os.path.exists(os.path.join(sticker_data_root_dir, d, 'info.json')):
                with open(os.path.join(sticker_data_root_dir, d, 'info.json'), 'r', encoding='utf-8') as f:
                    pack_info = json.load(f)
                    if pack_info['share_link_id'] == share_link_id:
                        norm_print(f"Found local metadata for pack named {pack_info['title']}!")
                        break
    else:
        # not found. download
        num_pack_id, title = get_num_pack_id_and_title(id_url)
        text_id = get_sticker_text_id(id_url)
        metadata = get_metadata(text_id)
        pack_info = {
            'title_kr': metadata['title'],
            'title': text_id,
            'text_id': text_id,
            'pack_id': num_pack_id,
            'share_link_id': share_link_id,
            'count': len(metadata['thumbnailUrls']),
            'archive_md5': None
        }
    sticker_count = pack_info['count']
    num_pack_id = pack_info['pack_id']
    title = pack_info['title']
    text_id = pack_info['text_id']
    scale_px = 0
    if output_fmt == 'webm':
        scale_px = 512

    norm_print('-----------------Sticker pack info:-----------------')
    norm_print('Title:', title)
    norm_print('Pack Number ID:', num_pack_id)
    norm_print('Share Link ID:', share_link_id)
    norm_print('Text ID:', text_id)
    norm_print('Total number of stickers:', sticker_count)
    if output_fmt == 'none':
        norm_print('Output format: <Download Only>')
    else:
        norm_print('Output format:', output_fmt)
    if scale_px:
        norm_print('Scale:', f'{scale_px}*{scale_px}px')
    norm_print('----------------------------------------------------')
    if not skip_confirmation:
        confirm = input('Do you wish to continue? Y/n: ')
        if confirm.lower() == 'n':
            norm_print('Aborting...')
            sys.exit(0)
        elif confirm and confirm.lower() != 'y':
            norm_print('Invalid input. Aborting...')
            sys.exit(1)

    # create folders
    sanitized_title = '_'.join(re.sub(r'[/:*?"<>|]', '', title).split())
    folder_name = sanitized_title
    sticker_pack_root = os.path.join(sticker_data_root_dir, folder_name)
    if not os.path.isdir(sticker_pack_root):
        os.mkdir(sticker_pack_root)

    sticker_dl_path = os.path.join(sticker_pack_root, 'dl')
    if not os.path.isdir(sticker_dl_path):
        os.mkdir(sticker_dl_path)

    sticker_process_temp_root = tempfile.mkdtemp()
    sticker_temp_store_extracted_zip_path = os.path.join(sticker_process_temp_root, 'extracted')

    # check if the archive has already been downloaded
    archive_path = os.path.join(sticker_dl_path, 'archive.zip')
    download_archive = True
    if os.path.exists(archive_path):
        # verify integrity using md5
        norm_print('Archive exists. Verifying integrity... ', end='')
        with open(archive_path, 'rb') as f:
            archive_content = f.read()
        archive_md5 = hashlib.md5(archive_content).hexdigest()
        if archive_md5 == pack_info['archive_md5']:
            norm_print('OK!')
            download_archive = False
        else:
            norm_print('Verification failed! Redownload...')
            os.remove(archive_path)
    if download_archive:
        # download sticker pack
        norm_print('Downloading sticker pack archive... ', end='')
        archive_content = get_sticker_archive(num_pack_id)
        norm_print('Complete!')
        # save archive and unzip to temp folder
        with open(archive_path, 'wb') as f:
            f.write(archive_content)
        # calculate md5 for future verification
        archive_md5 = hashlib.md5(archive_content).hexdigest()
        pack_info['archive_md5'] = archive_md5

    with open(os.path.join(sticker_pack_root, 'info.json'), 'w', encoding='utf-8') as f:
        json.dump(pack_info, f, ensure_ascii=False, indent=4)

    sticker_raw_path = os.path.join(sticker_pack_root, 'raw')

    if not os.path.isdir(sticker_raw_path):
        os.mkdir(sticker_raw_path)
        norm_print('Extracting archive... ', end='')
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            zip_ref.extractall(sticker_temp_store_extracted_zip_path)
        for fn in os.listdir(sticker_temp_store_extracted_zip_path):
            name, ext = os.path.splitext(fn)
            if ext in [".gif", ".webp"]:
                with open(os.path.join(sticker_temp_store_extracted_zip_path, fn), 'rb') as f:
                    decrypted = data_xor(f.read())
                with open(os.path.join(sticker_temp_store_extracted_zip_path, fn), 'wb') as f:
                    f.write(decrypted)
        norm_print('Complete!')

        # copy useful files to raw folder and rename

        for fn in os.listdir(sticker_temp_store_extracted_zip_path):
            shutil.copy(os.path.join(sticker_temp_store_extracted_zip_path, fn),
                        os.path.join(sticker_raw_path, fn))
        # cleanup temp folder
        shutil.rmtree(sticker_temp_store_extracted_zip_path)
    else:
        norm_print('Sticker pack already exists, skip unarchive...')

    if output_fmt == 'none':
        norm_print('No processing will be done, exit...')
        if open_folder:
            os.startfile(sticker_pack_root)
        sys.exit(0)

    # check dependency for processing
    if not shutil.which('magick'):
        print('Error: ImageMagick is missing. Please install missing dependencies are re-run the program')
        exit(1)

    # determine process option
    # TODO only webp is supported for now, but there are other formats like gif and png
    if output_fmt == 'gif':
        output_format = OutputFormat.GIF
    elif output_fmt == 'webm':
        output_format = OutputFormat.WEBM
    else:
        err_print(f'FAILED: Invalid output format {output_fmt}!')
        sys.exit(1)

    process_queue = Queue()

    if no_sub_dir:
        sticker_output_path = os.path.join(default_sticker_output_root_dir, sanitized_title)
    else:
        sticker_output_path = os.path.join(default_sticker_output_root_dir, sanitized_title,
                                           output_format.value)
    if scale_px:
        sticker_output_path += f'_scale_{scale_px}'
    if not os.path.isdir(sticker_output_path):
        os.makedirs(sticker_output_path)

    for i in range(sticker_count):
        # zero padding to 3 digits
        sticker_fn = f'{num_pack_id}.emot_{i + 1:03d}.webp'
        sticker_id = f'{num_pack_id}-{i + 1:03d}'
        in_pic = os.path.join(sticker_raw_path, sticker_fn)
        result_output = os.path.join(sticker_output_path, f'{num_pack_id}-{i + 1:03d}.{output_format.value}')
        operations = []
        if scale_px:
            operations.append(Operation.SCALE)
        if output_format == OutputFormat.GIF:
            operations.append(Operation.TO_GIF)
        elif output_format == OutputFormat.WEBM:
            operations.append(Operation.TO_WEBM)

        task = ProcessTask(sticker_id, in_pic, None, scale_px, operations, result_output)
        process_queue.put_nowait(task)

    processor = [KakaoProcessor(process_queue, sticker_process_temp_root, output_format) for _ in
                 range(num_process_threads)]

    for p in processor:
        p.start()

    if not quiet:
        with tqdm(total=sticker_count) as bar:
            last = sticker_count
            while not process_queue.empty():
                qsize = process_queue.qsize()
                bar.update(last - qsize)
                bar.refresh()
                last = qsize
                time.sleep(0.5)
            process_queue.join()
            bar.n = sticker_count
            bar.refresh()
            bar.clear()
    else:
        process_queue.join()

    norm_print('Process done!')

    # remove temp dir
    shutil.rmtree(sticker_process_temp_root)
    if open_folder:
        os.startfile(sticker_output_path)


if __name__ == '__main__':
    main()

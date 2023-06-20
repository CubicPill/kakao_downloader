import subprocess
import sys
import time

BATCH_FILE_PATH = 'batch.txt'


def main():
    all_links = []
    with open(BATCH_FILE_PATH) as f:
        for line in f:
            if line:
                all_links.append(line.strip())
    start_time = time.time()
    for link in all_links:
        print(f'Downloading: {link}')
        # strip query string
        link = link.split('?')[0]
        proc = subprocess.Popen(
                ['python', 'kakao_dl.py', link, '-q'] + sys.argv[1:],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            print(f'!!!Error downloading {link}, press enter to continue...')
            print(stdout.decode('utf-8', errors='ignore'))
            print(stderr.decode('utf-8', errors='ignore'))
    print(f'Finished in {int(time.time() - start_time)} seconds')


if __name__ == '__main__':
    main()

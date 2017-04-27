#coding:utf8

import time
import os
import logging

import config
from uploader import Uploader


logging.basicConfig(filename='ftp_upload.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
ch = logging.StreamHandler()
root.addHandler(ch)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def run():
    uploader = Uploader(config.TARGET_HOST, 22, config.TARGET_USERNAME, config.TARGET_PASSWORD, config.TARGET_DIR, config.NUM_UPLOAD_THREADS)

    logging.debug('started')

    while 1:
        files = [os.path.join(config.SRC_DIR, f) for f in os.listdir(config.SRC_DIR) if not f.startswith('_')]
        ret = uploader.upload(files)
        logger.info('found {} new files'.format(ret))

        time.sleep(config.LISTDIR_INTERVAL)


if __name__ == '__main__':
    run()

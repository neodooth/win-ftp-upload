#coding:utf8

from threading import Thread
import time
import os
import logging
from Queue import Queue
import paramiko


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Uploader(object):
    def __init__(self, target_host, target_port, target_username, target_password, target_dir, num_threads):
        # 队列，用来保存当前要上传的图片
        self.queue = Queue()
        # set，来防止重复入队已经在队列里的图片
        self.set = set()

        # 多线程。每个线程隔1秒启动，不然会出错
        self.threads = [Thread(target=self.run) for _ in xrange(num_threads)]
        for i in xrange(num_threads):
            logger.debug('lauching thread {} / {}'.format(i, num_threads))
            t = Thread(target=self.run)
            t.daemon = True
            t.start()
            time.sleep(1)

        self._target_host = target_host
        self._target_port = target_port
        self._target_username = target_username
        self._target_password = target_password
        self._target_dir = target_dir

    def upload(self, items):
        items = set(items)
        diff = items.difference(self.set)
        for i in diff:
            self.set.add(i)
            self.queue.put(i)
        logging.info('queue length: {}'.format(len(self.set)))
        return len(diff)

    def get_sftp(self):
        transport = paramiko.Transport((self._target_host, self._target_port))
        transport.connect(username=self._target_username, password=self._target_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.chdir(self._target_dir)
        return sftp

    def run(self):
        try:
            sftp = self.get_sftp()
        except Exception as e:
            logger.error('failed to start sftp: {}'.format(e), exc_info=True)
            raise e

        while 1:
            try:
                item = self.queue.get()
                try:
                    dst = os.path.split(item)[1]
                    tmpname = 'uploading_' + dst
                    logger.debug('uploading {} to {}'.format(item, dst))
                    sftp.put(item, tmpname)
                except:
                    logger.error('failed to upload: {}'.format(item), exc_info=True)

                try:
                    sftp.rename(tmpname, dst)
                except:
                    try:
                        sftp.remove(tmpname)
                    except:
                        pass

                try:
                    os.remove(item)
                except:
                    pass

                self.set.remove(item)
            except:
                pass

from threading import Thread
import easywebdav as wd
import time
import os
from datetime import datetime
import requests


class STACK(Thread):
    def __init__(self, id, queue, log_q, stack_parameters):
        Thread.__init__(self)
        self.stack = None
        self.id = id
        self.queue = queue
        self.log_q = log_q
        self.stack_parameters = stack_parameters

        self.__connect()

    def __log(self, message):
        self.log_q.append((self.id, datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'), message))

    def __connect(self):
        # This method does not need a try as this does not actually connect to the internet.
        self.stack = wd.connect(host=self.stack_parameters['host'],
                                protocol='https',
                                verify_ssl=True,
                                username=self.stack_parameters['username'],
                                password=self.stack_parameters['password'])

    def ls(self, remote):
        return self.stack.ls(remote)

    def upload(self, folder, file):
        try:
            self.__log('Uploading to STACK')
            self.stack.upload(file, "/remote.php/webdav/" + folder + '/' + file)
            self.__log('Successfully uploaded')
        except requests.exceptions.ConnectionError:
            self.__log('Connection Error')
            time.sleep(5)
            self.upload(folder, file)

    def run(self):
        while True:
            if not self.queue.empty():
                self.__connect()
                folder, file = self.queue.get()
                self.__log('Uploading ' + file)
                self.upload(folder, file)
                self.__log('Finished uploading ' + file)
                os.remove(file)
                if not self.queue.empty():
                    continue
                else:
                    time.sleep(30)
            else:
                time.sleep(1)
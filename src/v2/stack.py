from threading import Thread
import easywebdav as wd
import time
import os
from datetime import datetime
import requests


class STACK(Thread):
    def __init__(self, id, log_q, stack_parameters):
        Thread.__init__(self)
        self.stack = None
        self.id = id
        self.log_q = log_q
        self.stack_parameters = stack_parameters

        self.__connect()

    def __log(self, message):
        self.log_q.append((datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'), message))

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
            self.__log('Uploading ' + file + ' to STACK')
            self.stack.upload(file, "/remote.php/webdav/" + folder + '/' + file)
            self.__log('Successfully uploaded ' + file)
            return 'Done'
        except requests.exceptions.ConnectionError:
            self.__log('Connection Error')
            time.sleep(5)
            self.upload(folder, file)

    def download(self, folder, file):
        try:
            self.__log('Downloading ' + file + ' from STACK')
            self.stack.download("/remote.php/webdav/" + folder + '/' + file, file)
            self.__log('Successfully downloaded ' + file)
            return 'Done'
        except requests.exceptions.ConnectionError:
            self.__log('Connection Error')
            time.sleep(5)
            self.upload(folder, file)
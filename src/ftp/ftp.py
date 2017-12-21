import ftplib
import gzip
import os
import time
from collections import deque
from datetime import datetime
from threading import Thread

import easywebdav as wd

from ftp.incident_converter import IncidentConverter

'''''
Simple Thread that downloads files from the FTP server every minute and stores it to STACK for later analysis.
Also includes a logger that prints activities in the console 
'''''


class FTPStream(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.ftp = None
        self.ticks = 0
        self.files_to_download = ["trafficspeed.xml.gz",
                                  "traveltime.xml.gz",
                                  "incidents.xml.gz"]
        self.__connect_stack()

    def __log(self, message):
        self.queue.append((datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'), message))

    def __connect_ftp(self):
        try:
            self.__log('Connecting to FTP Server')
            self.ftp = ftplib.FTP('opendata.ndw.nu')
            self.ftp.set_debuglevel(2)
            self.__log('Logging into FTP Server')
            self.ftp.login()
        except Exception as e:
            self.__log(str(e))
            time.sleep(5)
            self.__log('Reattempting to make a connection')
            self.__connect_ftp()

    def __connect_stack(self):
        self.__log('Connecting to STACK')
        self.stack = wd.connect(host="ADDRESS", protocol="https", verify_ssl=True,
                                username='USERNAME', password='PASSWORD')

    def __download_ftp(self, f):
        try:
            self.__log('Downloading ' + str(f) + ' from FTP')
            timestr = time.strftime("%Y%m%d%H%M%S", time.localtime())
            file_name = f.split('.')[0] + timestr + '.xml.gz'
            file = open(file_name, 'wb')
            self.ftp.retrbinary('RETR %s' % f, file.write)
            file.close()
            return file_name
        except Exception as e:
            self.__log(str(e))
            time.sleep(5)
            self.__connect_ftp()
            self.__download_ftp(f)

    def __upload_stack(self, folder, file_name):
        try:
            self.__log('Uploading ' + file_name + ' to STACK')
            self.stack.upload(file_name, "/remote.php/webdav/" + folder + '/' + file_name)
        except Exception as e:
            self.__log(str(e))
            time.sleep(5)
            self.__connect_stack()
            self.__upload_stack(folder, file_name)

    def run(self):
        while True:
            t0 = time.time()

            # Reconnect very 10 minutes to prevent timeouts, 10 min is arbitrary.
            if self.ftp is None or self.ticks % 10 == 0:
                self.__log('(Re)Connecting to FTP Server')
                self.__connect_ftp()

            for f in self.files_to_download:
                file_name = self.__download_ftp(f)

                if f == "incidents.xml.gz":
                    self.__log('Extracting incidents.xml.gz')
                    with gzip.open(file_name, 'rb') as compressed:
                        with open(file_name[:-3], 'wb') as decompressed:
                            for line in compressed:
                                decompressed.write(line)

                    self.__log('Processing incidents.xml.gz')
                    IncidentConverter(file_name[:-3])
                    os.remove(file_name[:-3])  # Decompressed file
                    self.__upload_stack(f.split('.')[0] + '/processed', file_name[:-6] + 'txt')
                    os.remove(file_name[:-6] + 'txt')

                self.__upload_stack(f.split('.')[0], file_name)
                os.remove(file_name)

            # Wait for a minute
            self.__log('Running for ' + str(self.ticks) + ' minutes')
            executing_time = 60 - (time.time() - t0)
            if executing_time > 0:
                time.sleep(60 - (time.time() - t0))
            self.ticks += 1


class Logger(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            if len(self.queue) != 0:
                t, message = self.queue.popleft()
                with open('log.txt', 'a') as log:
                    log.write(t + ' ' + message + '\n')
                    log.flush()

                # Clear the console
                os.system('cls' if os.name == 'nt' else 'clear')
                with open('log.txt', 'r') as log:
                    lines = log.readlines()
                print('|---------------------------------------------------------------|')
                for line in lines[-20:]:
                    print("| " + line[:-1])
            else:
                time.sleep(5)


try:
    os.remove('log.txt')
except Exception as e:
    print(e)

log_q = deque(maxlen=20)
logger = Logger(log_q)
logger.start()

stream = FTPStream(log_q)
stream.start()

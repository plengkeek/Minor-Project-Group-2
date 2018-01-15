import ftplib
import gzip
import os
import time
from threading import Thread
from queue_mod import queue

import easywebdav as wd

from csv_converter import CSVConverter
from incident_converter import IncidentConverter

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

    def __connect_ftp(self):
        try:
            self.ftp = ftplib.FTP('opendata.ndw.nu')
            self.ftp.set_debuglevel(2)
            self.ftp.login()
        except Exception as e:
            time.sleep(5)
            self.__connect_ftp()

    def __connect_stack(self):
        self.stack = wd.connect(host="ADDRESS", protocol="https", verify_ssl=True,
                                username='USERNAME', password='PASSWORD')

    def __download_ftp(self, f):
        try:
            timestr = time.strftime("%Y%m%d%H%M%S", time.localtime())
            file_name = f.split('.')[0] + timestr + '.xml.gz'
            file = open(file_name, 'wb')
            self.ftp.retrbinary('RETR %s' % f, file.write)
            file.close()
            return file_name
        except Exception as e:
            time.sleep(5)
            self.__connect_ftp()
            self.__download_ftp(f)


    def run(self):
        while True:
            t0 = time.time()

            # Reconnect very 10 minutes to prevent timeouts, 10 min is arbitrary.
            if self.ftp is None or self.ticks % 10 == 0:
                self.__connect_ftp()

            for f in self.files_to_download:
                file_name = self.__download_ftp(f)

                if f == "incidents.xml.gz":
                    with gzip.open(file_name, 'rb') as compressed:
                        with open(file_name[:-3], 'wb') as decompressed:
                            for line in compressed:
                                decompressed.write(line)

                    IncidentConverter(file_name[:-3])
                    os.remove(file_name[:-3])  # Decompressed file
                    os.remove(file_name)  # Downloaded file
                    self.queue.put(str(file_name[:-6] + 'txt'))

                else:
                    with gzip.open(file_name, 'rb') as compressed:
                        with open(file_name[:-3], 'wb') as decompressed:
                            for line in compressed:
                                decompressed.write(line)
                    CSVConverter(file_name[:-3])
                    os.remove(file_name[:-3])  # Decompressed file
                    os.remove(file_name)  # Downloaded file
                    self.queue.put(str(file_name[:-6]) + 'txt')

            # Wait for a minute
            executing_time = 60 - (time.time() - t0)
            if executing_time > 0:
                time.sleep(60 - (time.time() - t0))
            self.ticks += 1


files_queue = queue()
stream = FTPStream(files_queue)
stream.start()
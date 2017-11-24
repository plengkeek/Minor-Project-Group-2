import ftplib
from threading import Thread
import time, os
import easywebdav as wd


'''''
Simple Thread that downloads files from the FTP server every minute and stores it to STACK for later analysis.
'''''


class FTPStream(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.ftp = None
        self.stack = None
        self.ticks = 0
        self.files_to_download = ["trafficspeed.xml.gz",
                                  "traveltime.xml.gz",
                                  "incidents.xml.gz"]

    def __connect_ftp(self):
        self.ftp = ftplib.FTP('opendata.ndw.nu')
        self.ftp.login()

    def __connect_stack(self):
        self.stack = wd.connect(host="plengkeek.stackstorage.com", protocol="https", verify_ssl=True,
                                username='projectgroup', password='wearethebest')

    def run(self):
        while True:
            t0 = time.time()

            # Reconnect very 10 minutes to prevent timeouts, 10 min is arbitrary.
            if self.ftp is None or self.ticks % 10 == 0:
                self.__connect_ftp()
            if self.stack is None or self.ticks % 10 == 0:
                self.__connect_stack()

            for f in self.files_to_download:
                # Download a file from the server
                timestr = time.strftime("%Y%m%d%H%M%S", time.localtime())
                file_name = f.split('.')[0] + timestr + '.xml.gz'
                file = open(file_name, 'wb')
                self.ftp.retrbinary('RETR %s' % f, file.write)
                file.close()

                # Uploading to STACK
                self.stack.upload(file_name, "/remote.php/webdav/" + f.split('.')[0] + '/' + file_name)
                os.remove(file_name)

            # Wait for a minute
            print('Running for ' + str(self.ticks) + ' minutes')
            time.sleep(60 - (time.time() - t0))

            self.ticks += 1


stream = FTPStream()
stream.start()




import ftplib
from threading import Thread
import time, os
import easywebdav as wd


'''''
Simple Thread that downloads a file from the FTP server every minute.
'''''


class FTPStream(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.ftp = None
        self.stack = None

        self.__connect_ftp()
        self.__connect_stack()

    def __connect_ftp(self):
        self.ftp = ftplib.FTP('opendata.ndw.nu')
        self.ftp.login()

    def __connect_stack(self):
        self.stack = wd.connect(host="plengkeek.stackstorage.com", protocol="https", verify_ssl=True,
                                username='projectgroup', password='wearethebest')

    def run(self):
        while True:
            # Download a file from the server
            print('Downloading File...')
            timestr = time.strftime("%Y%m%d%H%M%S", time.localtime())
            file_name = 'trafficspeed' + timestr + '.xml.gz'
            file = open(file_name, 'wb')
            self.ftp.retrbinary('RETR %s' % 'trafficspeed.xml.gz', file.write)
            file.close()

            print('Uploading to STACK')
            self.stack.upload(file_name, "/remote.php/webdav/livedata/" + file_name)
            os.remove(file_name)

            # Wait for a minute
            print('Sleeping for 60 seconds')
            time.sleep(60)


stream = FTPStream()
stream.start()




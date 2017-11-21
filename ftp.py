import ftplib
import gzip
from threading import Thread
import time

'''''
Simple Thread that downloads a file from the FTP server every minute.
'''''


class FTPStream(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.ftp = None

        self.__connect()

    def __connect(self):
        self.ftp = ftplib.FTP('opendata.ndw.nu')
        self.ftp.login()

    def run(self):
        while True:
            # Download a file from the server
            print('Downloading File...')
            timestr = time.strftime("%Y%m%d%H%M%S", time.localtime())
            file = open('trafficspeed' + timestr + '.xml.gz', 'wb')
            self.ftp.retrbinary('RETR %s' % 'trafficspeed.xml.gz', file.write)
            file.close()

            # Extract the file and save as xml
            with gzip.open('trafficspeed' + timestr + '.xml.gz', 'rb') as file_in:
                with open('trafficspeed' + timestr + '.xml', 'wb') as file_out:
                    for line in file_in:
                        file_out.write(line)

            # Close all the files
            file_in.close()
            file_out.close()

            # Wait for a minute
            print('Sleeping for 60 seconds')
            time.sleep(60)


stream = FTPStream()
stream.start()




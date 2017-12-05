import ftplib
from threading import Thread
import time, os
import easywebdav as wd
import gzip
from incident_converter import IncidentConverter


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
        try:
            self.ftp = ftplib.FTP('opendata.ndw.nu')
            self.ftp.login()
        # Timeout or loss of internet connection
        except :
            print("Failed to connect to the ftp server")
            time.sleep(5)
            self.__connect_ftp()

    def __connect_stack(self):
        try:
            self.stack = wd.connect(host="ADDRESS", protocol="https", verify_ssl=True,
                                    username='NAME', password='PASS')
        # Timeout or loss of internet connection
        except:
            print("Failed to connect to STACK")
            time.sleep(5)
            self.__connect_stack()

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

                if f == "incidents.xml.gz":
                    with gzip.open(file_name, 'rb') as compressed:
                        with open(file_name[:-3], 'wb') as decompressed:
                            for line in compressed:
                                decompressed.write(line)

                    IncidentConverter(file_name[:-3])
                    os.remove(file_name) # Downloaded file
                    os.remove(file_name[:-3]) # Decompressed file
                    file_name = file_name[:-6] + 'txt' # name of processed file
                    try:
                        self.stack.upload(file_name, "/remote.php/webdav/" + f.split('.')[0] + '/processed/' + file_name)
                    except:
                        print('Failed to upload')
                        time.sleep(5)
                        pass

                try:
                    self.stack.upload(file_name, "/remote.php/webdav/" + f.split('.')[0] + '/' + file_name)
                    os.remove(file_name)
                except:
                    print('Failed to upload')
                    time.sleep(5)
                    pass

            # Wait for a minute
            print('Running for ' + str(self.ticks) + ' minutes', end='\r')
            time.sleep(60 - (time.time() - t0))

            self.ticks += 1


stream = FTPStream()
stream.start()




import ftplib
import gzip

'''''
# FTP code snippets
'''''

# Connect to server and print the available files.
ftp = ftplib.FTP('opendata.ndw.nu')
ftp.login()
print(ftp.dir())

# Download a file from the server
file = open('trafficspeed.xml.gz', 'wb')
ftp.retrbinary('RETR %s' % 'trafficspeed.xml.gz', file.write)
file.close()

# Extract the file and save as xml
with gzip.open('trafficspeed.xml.gz', 'rb') as file_in:
    with open('trafficspeed.xml', 'wb') as file_out:
        for line in file_in:
            file_out.write(line)


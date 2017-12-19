import os

''''
Simple code to extract a single location out of the txts
'''
''''
dir = 'D:/01-01-2017/txts'
data = []
datafile = open('data.txt', 'w')
for file in os.listdir('D:/01-01-2017/txts'):
    datafile.write(open(dir + '/' + file).readline())

datafile.close()
'''

import zipfile
import os

dir = 'D:/STACK/historicaldata/processedintensityandspeed'
data = []
datafile = open('data.txt', 'wb')

for file in os.listdir(dir):
    if '.zip' in file:
        zip = zipfile.ZipFile(dir + '/' + file)
        for textfile in zip.namelist():
            datafile.write(zip.open(textfile).readline())

datafile.close()

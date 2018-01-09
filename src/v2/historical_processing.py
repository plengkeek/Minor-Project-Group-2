import os
import shutil
import subprocess
import time
import zipfile
from collections import deque
from datetime import datetime

from logger import Logger
from multi_converter import *

from stack import STACK

if __name__ == '__main__':

    path_7zip = r"C:\Program Files\7-Zip\7z.exe"

    def log(log_q, message):
        log_q.append((datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'), message))


    log_queue = deque(maxlen=30)

    stack_parameters = {'host': 'plengkeek.stackstorage.com',
                        'username': 'projectgroup',
                        'password': 'wearethebest'}

    stack = STACK(0, log_queue, stack_parameters)

    logger = Logger(log_queue)
    logger.start()

    while True:
        available = set([file[0][-14:] for file in stack.ls('remote.php/webdav/historicaldata/intensityandspeed')[1:]])
        done = set([file[0][-14:] for file in stack.ls('remote.php/webdav/historicaldata/processedintensityandspeed')[1:]])
        to_be_processed = sorted(list(available - done), key=lambda x: datetime.strptime(x[:-4], "%d-%m-%Y"))

        if stack.download('historicaldata/intensityandspeed', to_be_processed[0]) == 'Done':
            log(log_queue, 'Extracting ' + to_be_processed[0])
            zip = zipfile.ZipFile(to_be_processed[0], 'r')
            for file in zip.namelist():
                if file.startswith(to_be_processed[0][:-4] + '/'):
                    zip.extract(file, './')
            zip.close()

        log(log_queue, 'Converting ' + to_be_processed[0])
        multiconverter('./' + to_be_processed[0][:-4] + '/')

        log(log_queue, 'Zipping ' + to_be_processed[0])
        outfile_name = "p" + to_be_processed[0]
        subprocess.call([path_7zip, "a", "-tzip", outfile_name, './' + to_be_processed[0][:-4] + '/txts/' + "*.txt"])

        if stack.upload('historicaldata/processedintensityandspeed', 'p' + to_be_processed[0]) == 'Done':
            os.remove('p' + to_be_processed[0])
            shutil.rmtree('./' + to_be_processed[0][:-4])
            os.remove(to_be_processed[0])
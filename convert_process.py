from flow_speed_converter import FlowSpeedConverter
import os
import gzip
import shutil
from threading import Thread
import multiprocessing as mp


def convert(file):
    # decompress file
    with gzip.open(file, "rb") as f_in:
        with open(file[:-2] + "xml", "wb") as f_out:
            f_out.write(f_in.read())

    # remove .gz file
    os.remove(file)

    # change .gz extension to .xml
    file = file[:-2] + "xml"

    # process file
    FlowSpeedConverter(file)

    # remove .xml file
    os.remove(file)

    # change to .txt extension
    file = file[:-3] + "txt"

    # upload path
    path = file.split("/")
    path = path[1]

    # create folder
    try:
        os.makedirs(
            "/home/thijs-gerrit/stack/processed_historical/intensityandspeed/"
            + path)
    except:
        pass

    # upload
    shutil.move(
        file,
        "/home/thijs-gerrit/stack/processed_historical/intensityandspeed/" +
        path)

    with open("finished_files.txt", "a") as log:
        log.write(path + ".zip")


class ConvertThread(Thread):
    def __init__(self, next_process):
        Thread.__init__(self)
        self._next_process = next_process

    def run():
        worker = mp.Process(target=convert, args=(self._next_process))
        worker.start()
        worker.join()

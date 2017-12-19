import subprocess
import os
from zipfile import ZipFile
import glob
from threading import Thread
import multiprocessing as mp


def new_zip(file, queue):
    """
    Manage the download of a new zipfile, unzips it and adds it contents to
    the process queue
    """

    # download the file
    subprocess.run(["python2", "download_file.py", file])

    # extract the contents
    zip = ZipFile("_cache/" + file)
    zip.extractall(path="_cache/")

    # delete the zip
    os.remove("_cache/" + file)

    print("filling queue")
    # add the files to the process queue
    files = glob.glob("_cache/" + file[:-4] + "/**/*.gz", recursive=True)

    for file in files:
        queue.put(file)

    queue.put("DONE")
    print("done new zip")


class ZipClass(Thread):
    def __init__(self, next_download, process_q):
        Thread.__init__(self)
        self._next_download = next_download
        self._process_q = process_q

    def run():
        worker = mp.Process(target=new_zip, args=(self._next_download, self._process_q))
        worker.start()
        worker.join()

"""
This script handles the downloading, uploading and processing of the traffic
XML files.
"""

import subprocess
import multiprocessing as mp
import os
from queue import Queue
from new_zip import ZipClass
import time
from convert_process import ConvertThread

if __name__ == "__main__":
    # prompt the user for the number of cpu cores to be used
    no_of_cpus = int(input("How many CPU cores should be used? "))

    # initialize values
    is_running = True
    first_delete = True

    # First off, check which files need to be processed at this moment.
    # The script that checks the STACK server uses a Python 2 library,
    # therefore the script is called via the subprocess module
    completed_process = subprocess.run(["python2", "check_stack_dirs.py"])

    # create download and upload queue's
    download_q = mp.Queue()

    # initialize an empty process queue
    process_q = mp.Queue()
    process_q.put("DONE")

    # fill the download Queue
    with open("to_download.txt") as file:
        for file in file.readlines():
            print(file)
            download_q.put(file[:-1])

    # put an explicit end in the download Queue
    download_q.put("DONE")

    while True:
        print(len(mp.active_children()))
        # check if there is an open spot to start a new process
        if len(mp.active_children()) < no_of_cpus:
            next_process = process_q.get()
            print(next_process)

            # if the current folder has been processed, initiate a new download
            if next_process == "DONE":
                if not first_delete:
                    os.remove("_cache/" + current_folder)
                    first_delete = False

                next_download = download_q.get()
                current_folder = next_download[:-4]

                # check if the download queue is not empty
                if next_download != "DONE":
                    print("Starting a new download...")
                    thread = ZipClass(next_download, process_q)
                    thread.start()

            # current folder is not yet finished processing, continue with it
            else:
                print("Starting a new process...")
                thread = ConvertThread(next_process)
                thread.start()
        else:
            print("...")

import glob
import multiprocessing as mp
from functools import partial

from csv_converter import CSVConverter


def writer(queue, dir_path):
    """"This method puts a list of of compressed .xml files (*.gz) in the queue to process"""

    # check if the directory ends with an slash or not, the path needs to be corrected for this
    if dir_path[-1] == '/':
        path = dir_path + '**/*.gz'
    else:
        path = dir_path + '/**/*.gz'

    # retrieve all the .gz files in the specified directory and its subdirectories
    files = glob.glob(path, recursive=True)

    # put all the files in the queue
    for f in files:
        queue.put(f)

    # put a value in the queue to specify the end of the queue has been reached.
    # supposedly more reliably than queue.is_empty()
    queue.put('DONE')


def get_worker_id(no_of_cpus, x):
    """A method to assign a worker id to each process.
    This is just to track the various processes in a log"""
    x += 1
    if x > no_of_cpus:
        return 1
    else:
        return x


def process_function(file):
    """Call the function that needs to be performed.
    Override this function to process different .xml files.
    NOTE: they should have this signature: Converter(file: string) -> None"""
    CSVConverter(file)


def multiconverter(dir):
    # initialize values
    is_running = True
    empty_queue = False
    i = 0
    no_of_cpus = 20
    no_of_processes = 0

    worker_id = partial(get_worker_id, no_of_cpus)
    process_queue = mp.Queue()

    # fill the queue
    writer(process_queue, dir)

    while is_running:
        # check if all the processes are done
        if empty_queue:
            if len(mp.active_children()) == 0:
                is_running = False

        # check if there is an open spot to start a new process
        elif len(mp.active_children()) < no_of_cpus:
            next_process = process_queue.get()

            # check if the next process is not the end of the queue
            if next_process != "DONE":
                no_of_processes += 1
                i = worker_id(i)

                # start a new process and execute it
                print("[NEW PROCESS] Worker " + str(i) + " on " + next_process)
                worker = mp.Process(target=process_function, name="Worker " + str(i), args=(next_process,))
                worker.start()
            else:
                # the end of the queue is reached
                print('No more processes to run. Wait for workers to finish..')
                empty_queue = True

    print('done')
    return 'Done'

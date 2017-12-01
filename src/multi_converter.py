import multiprocessing as mp
from csv_converter import CSVConverter
from functools import partial
import timeit


def writer(queue):
    """"Writes list of files that are processed"""
    files = ['two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']*8

    for file in files:
        queue.put('../example_data/' + file + '.xml')

    queue.put('DONE')


def get_worker_id(no_of_cpus, x):
    x += 1
    if x > no_of_cpus:
        return 1
    else:
        return x


def process_function(file):
    CSVConverter(file)


if __name__ == '__main__':
    start_t = timeit.default_timer()

    is_running = True
    empty_queue = False
    i = 0
    no_of_cpus = 6
    no_of_processes = 0

    worker_id = partial(get_worker_id, no_of_cpus)
    process_queue = mp.Queue()
    writer(process_queue)

    while is_running:
        if empty_queue:
            if len(mp.active_children()) == 0:
                is_running = False

        elif len(mp.active_children()) < no_of_cpus:
            next_process = process_queue.get()

            if next_process != "DONE":
                no_of_processes += 1
                i = worker_id(i)

                print("[NEW PROCESS] Worker " + str(i) + " on " + next_process)
                worker = mp.Process(target=process_function, name="Worker " + str(i), args=(next_process,))
                worker.start()
            else:
                print('No more processes to run. Wait for workers to finish..')
                empty_queue = True

    print(timeit.default_timer() - start_t)
    print((timeit.default_timer() - start_t) / no_of_processes)

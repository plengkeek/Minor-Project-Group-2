from threading import Thread
from multiprocessing import Process, active_children
from process_queue import ProcessQueue
from csv_converter import CSVConverter


class ConvertThread(Thread):
    def __init__(self, process_queue: ProcessQueue):
        Thread.__init__(self)
        self._process_queue = process_queue

        self.no_of_cpus = 8

    def run(self):
        def worker_id(no_of_cpus, i):
            i += 1

            if i > no_of_cpus:
                return 1
            else:
                return i

        no_of_processes = 0
        current_id = 0

        while True:
            if len(active_children()) < self.no_of_cpus:
                next_file = self._process_queue.get()

                no_of_processes += 1
                current_id = worker_id(self.no_of_cpus, current_id)
                print("[NEW PROCESS][No: {0}] Worker {1} on {2}".format(no_of_processes, current_id, next_file))

                worker = Process(target=CSVConverter, name="Worker " + str(current_id), args=(next_file,))
                worker.start()

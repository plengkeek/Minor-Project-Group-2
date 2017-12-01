from convert_thread import ConvertThread
from stack_thread import STACKThread
from process_queue import ProcessQueue
from download_queue import DownloadQueue
from threading import Event


def main():
    # create the means to communicate between the threads: Queue's and Events
    download_queue = DownloadQueue()
    process_queue = ProcessQueue()
    download_trigger = Event()

    # create the thread objects
    convert_thread = ConvertThread(process_queue)
    stack_thread = STACKThread(download_queue, download_trigger)

    # add the stack thread as a listener to the process queue
    process_queue.register_stack_thread(stack_thread)

    # start the threads
    convert_thread.run()
    stack_thread.run()

if __name__ == "__main__":
    main()

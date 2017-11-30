from multiprocessing import Queue
from threading import Lock


class ProcessQueue(Queue):
    """Modified Queue. The put and get methods are protected with a lock to make it threadsafe.
    As such, a ProcessQueue object can only be accessed by one thread at the time."""
    def __init__(self):
        Queue.__init__(self)
        self.__lock = Lock()
        self.__stack_thread = None

    def put(self, obj, block=True, timeout=None):
        with self.__lock:
            Queue.put(self, obj, block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        with self.__lock:
            if self.__stack_thread is not None:
                self.__stack_thread.download_trigger.set()
                Queue.get(self, block=block, timeout=timeout)

    def register_stack_thread(self, thread):
        self.__stack_thread = thread

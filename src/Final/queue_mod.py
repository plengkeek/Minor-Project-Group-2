from multiprocessing import Queue


class queue():
    def __init__(self):
        self.__queue = Queue()
        self.__counter = 0

    def put(self, item):
        self.__queue.put(item)
        self.__counter += 1

    def get(self):
        if self.size() != 0:
            self.__counter += -1
            return self.__queue.get()
        else:
            return Exception('Queue is empty')

    def size(self):
        return self.__counter

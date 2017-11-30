from queue import Queue
from datetime import date


class DownloadQueue(Queue):
    def __init__(self):
        Queue.__init__(self)
        self.__log = "download_log.txt"

    def fill(self, stack):
        with open(self.__log) as log:
            date_as_string = log.readline()

        date_as_list = date_as_string.split('-')
        date_as_list = list(map(int, date_as_list))
        last_date = date(date_as_list[2], date_as_list[1], date_as_list[0])
"""
STRUCTURE OF LOG:
[last downloaded .zip]
"""

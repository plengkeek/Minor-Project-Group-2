from queue import Queue
from datetime import date as Date


class DownloadQueue(Queue):
    def __init__(self):
        Queue.__init__(self)
        self._log = "log.txt"

    def fill(self, dirs):
        with open(self._log) as log:
            date_as_string = log.readline()

        date_as_list = date_as_string.split('-')
        date_as_list = list(map(int, date_as_list))
        last_date = Date(date_as_list[2], date_as_list[1], date_as_list[0])

        for date in dirs:
            file = date

            # remove folder path
            date = date.split("/")[-1]
            # remove extension
            date = date.split(".")[0]
            # convert to list of int
            date = list(map(int, date.split("-")))

            date = Date(date[2], date[1], date[0])

            if date > last_date:
                self.put(file)
"""
STRUCTURE OF LOG:
[last downloaded .zip]
"""

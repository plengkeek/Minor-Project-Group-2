import requests


class KNMI:
    def __init__(self):
        self.stations = [209, 210, 215, 225, 235, 240, 242, 248, 249, 251, 257, 258, 260, 265, 267, 269, 270, 273, 275,
                        277, 278, 279, 280, 283, 285, 286, 290, 308, 310, 311, 312, 313, 315, 316, 319, 323, 324, 330,
                        331, 340, 343, 344, 348, 350, 356, 370, 375, 377, 380, 391]
        self.post_data = ''
        self.url = 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi'

    def __add_stations(self, stations):
        # Add requested stations to the post_data
        self.post_data += 'stns='
        if stations != 'ALL':

            if len([i for i in stations if i in self.stations]) != len(stations):
                raise Exception('Requesting non excising stations!')

            for station in stations:
                self.post_data += str(station) + ':'
            self.post_data = self.post_data[:-1]
            self.post_data += '&'
        else:
            self.post_data += 'ALL'

    def __add_time(self, start_date, end_date):
        self.post_data += 'start='
        self.post_data += start_date + '&'
        self.post_data += 'end='
        self.post_data += end_date

    def download(self, file_name, stations, start_date, end_date):
        self.__add_stations(stations)
        self.__add_time(start_date, end_date)

        print(self.post_data)

        print('Downloading...')
        r = requests.post(self.url, self.post_data)

        print('Writing to file...')
        file = open(file_name, 'w')
        file.writelines(r.text)
        file.close()


knmi = KNMI()
knmi.download(file_name='knmiweatherdata.txt', stations='ALL', start_date='2017010101', end_date='')

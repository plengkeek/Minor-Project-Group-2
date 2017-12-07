from csv_converter import CSVConverter
from functools import partial

class HistoricalConverter(CSVConverter):
    def __init__(self, file):
        CSVConverter.__init__(self, file)

    def _process(self):
        situation_root = self._xpath(self._xml_tree, ".//d:measurementSiteRecord")

        for i in range(len(situation_root)):
            situation_xpath = partial(self._xpath, situation_root[i])

            measurement_tech = situation_xpath(".//d:computationMethod")[0].text

            latitude_txt = situation_xpath(".//d:latitude")[0].text
            longitude_txt = situation_xpath(".//d:longitude")[0].text
            name = situation_xpath(".//d:measurementSiteName")[0].text
            lanes = situation_xpath(".//d:measurementSiteNumberOfLanes")[0].text

            self._buffer_to_write.append(str(name) + ";")
            self._buffer_to_write.append(str(latitude_txt) + ";")
            self._buffer_to_write.append(str(longitude_txt) + ";")
            self._buffer_to_write.append(str(lanes)+";")



HistoricalConverter('F:\\Minor project DATA\\sensors.xml')

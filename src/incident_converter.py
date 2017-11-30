from csv_converter import CSVConverter
from functools import partial

class IncidentConverter(CSVConverter):
    def __init__(self, file):
        CSVConverter.__init__(self, file)

    def _process(self):
        situation_root = self._xpath(self._xml_tree, "//d:situation")

        for i in range(len(situation_root)):
            situation_xpath = partial(self._xpath, situation_root[i])

            time_txt = situation_xpath("./d:situationVersionTime")[0].text
            latitude_txt = situation_xpath("./d:situationRecord/d:groupOfLocations/d:locationForDisplay/d:latitude")[0].text
            longitude_txt = situation_xpath("./d:situationRecord/d:groupOfLocations/d:locationForDisplay/d:longitude")[0].text

            try:
                obstruction_txt = situation_xpath("./d:situationRecord/d:vehicleObstructionType")[0].text
            except IndexError:
                obstruction_txt = "other"

            self._buffer_to_write.append(time_txt + ";")
            self._buffer_to_write[i] += str(latitude_txt) + ";"
            self._buffer_to_write[i] += str(longitude_txt) + ";"
            self._buffer_to_write[i] += str(obstruction_txt)


# print(IncidentConverter('incidents.xml')._buffer_to_write)

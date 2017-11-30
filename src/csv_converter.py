from lxml import etree
from functools import partial


class CSVConverter:
    def __init__(self, file=None):
        self._buffer_to_write = []
        # self.path =

        try:
            self._xml_tree = etree.parse(file)
            self._process()
            self._write_to_file(file)

        except OSError:
            print("Couldn't open the .xml file: " + file)

    def _write_to_file(self, file):
        file = file.split('\\')
        file = file[-1]
        file = "F:\Minor project DATA\Converted\\" + file[:-3]
        file += 'txt'

        with open(file, 'w') as write_file:
            write_file.write('\n'.join(self._buffer_to_write))
            # write_file.writelines(self.__buffer_to_write)

    def _process(self):
        """Process the data by extracting the data from the XML file and writing it to a buffer"""
        sites_root = self._xpath(self._xml_tree, "//d:siteMeasurements")

        for i in range(len(sites_root)):
            # create a partial function
            site_xpath = partial(self._xpath, sites_root[i])

            # get measured values elements
            measured_values = self._xpath(sites_root[i], ".//d:basicData")

            # get the elements containing the time and id
            time_reference = site_xpath("./d:measurementTimeDefault")[0]    # get measurement time default element
            site_reference = site_xpath("./d:measurementSiteReference")[0]  # get site reference element

            # append time and id to the buffer
            self._buffer_to_write.append(time_reference.text + ";")
            self._buffer_to_write[i] += site_reference.attrib["id"] + ";"

            for v in measured_values:
                # check if the measured value is average speed
                if v.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"] == "TrafficSpeed":
                    speed = self._xpath(v, ".//d:speed")[0]
                    self._buffer_to_write[i] += "-2," + speed.text + ";"
                # check if the measured value is traffic flow
                elif v.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"] == "TrafficFlow":
                    flow = self._xpath(v, ".//d:vehicleFlowRate")[0]
                    self._buffer_to_write[i] += flow.text + ",-2;"

    @staticmethod
    def _xpath(root, path):
        # namespaces dictionary
        ns = {"soap": "http://schemas.xmlsoap.org/soap/envelope/",
              "d": "http://datex2.eu/schema/2/2_0"}

        return root.xpath(path, namespaces=ns)

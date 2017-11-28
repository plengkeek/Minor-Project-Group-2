from lxml import etree
from functools import partial


class CSVConverter:
    def __init__(self, file, type=0):
        self.__buffer_to_write = []

        try:
            self.__xml_tree = etree.parse(file)
            print("Loaded XML file")

            self.__process()
            self.__write_to_file(file)

        except OSError:
            print("Couldn't open the XML file")

    def __write_to_file(self, file):
        file = file[:-3]
        file += 'txt'

        with open(file, 'w') as write_file:
            write_file.write('\n'.join(self.__buffer_to_write))
            # write_file.writelines(self.__buffer_to_write)

        print("Done writing data to TXT file")

    def __process(self):
        """Process the data by extracting the data from the XML file and writing it to a buffer"""
        sites_root = self.__xpath(self.__xml_tree, "//d:siteMeasurements")

        for i in range(len(sites_root)):
            site_xpath = partial(self.__xpath, sites_root[i])

            # get the relevant data from the .xml file
            site_reference = site_xpath("./d:measurementSiteReference")[0]  # get site reference element
            vehicle_flow_rates = site_xpath(".//d:vehicleFlowRate")         # get vehicle flow rate element

            # append the data to the buffer
            self.__buffer_to_write.append(site_reference.attrib["id"])

            for v in vehicle_flow_rates:
                if int(v.text) > 0:
                    self.__buffer_to_write[i] += ',' + v.text

        print("Done processing the content")

    @staticmethod
    def __xpath(root, path):
        # namespaces dictionary
        ns = {"soap": "http://schemas.xmlsoap.org/soap/envelope/",
              "d": "http://datex2.eu/schema/2/2_0"}

        return root.xpath(path, namespaces=ns)

CSVConverter('Snelheden_Intensiteiten.xml')

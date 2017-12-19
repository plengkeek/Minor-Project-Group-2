from lxml import etree
import abc


class BaseConverter(abc.ABC):
    def __init__(self, file=None):
        self._buffer_to_write = []

        try:
            self._xml_tree = etree.parse(file)
            self._process()
            self._write_to_file(file)

        except OSError:
            print("Couldn't open the .xml file: " + file)

    def _write_to_file(self, file):
        file = file[:-3]
        file += 'txt'

        with open(file, 'w') as write_file:
            write_file.write('\n'.join(self._buffer_to_write))

    @staticmethod
    def _xpath(root, path):
        # namespaces dictionary
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "d": "http://datex2.eu/schema/2/2_0"
        }

        return root.xpath(path, namespaces=ns)

    @abc.abstractmethod
    def _process(self):
        pass

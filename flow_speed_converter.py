from base_converter import BaseConverter
from functools import partial


class FlowSpeedConverter(BaseConverter):
    def __init__(self, file):
        super().__init__(file)

    def _process(self):
        sites_root = self._xpath(self._xml_tree, "//d:siteMeasurements")

        for i in range(len(sites_root)):
            # create a partial function
            site_xpath = partial(self._xpath, sites_root[i])

            # get measured values elements
            measured_values = self._xpath(sites_root[i], ".//d:basicData")

            # get the elements containing the time and id
            time_reference = site_xpath("./d:measurementTimeDefault")[
                0]  # get measurement time default element
            site_reference = site_xpath("./d:measurementSiteReference")[
                0]  # get site reference element

            # append time and id to the buffer
            self._buffer_to_write.append(time_reference.text + ";")
            self._buffer_to_write[i] += site_reference.attrib["id"] + ";"

            for v in measured_values:
                # check if the measured value is average speed
                if v.attrib[
                        "{http://www.w3.org/2001/XMLSchema-instance}type"] == "TrafficSpeed":
                    speed = self._xpath(v, ".//d:speed")[0]
                    self._buffer_to_write[i] += "-2;" + speed.text + ";"
                # check if the measured value is traffic flow
                elif v.attrib[
                        "{http://www.w3.org/2001/XMLSchema-instance}type"] == "TrafficFlow":
                    flow = self._xpath(v, ".//d:vehicleFlowRate")[0]
                    self._buffer_to_write[i] += flow.text + ";-2;"

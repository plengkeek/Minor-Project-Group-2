import os
import numpy as np
import scipy.cluster.vq as clust
import pygmaps
import webbrowser


file_path = 'C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\GitHub\src\Historical\sensors_results\Converted\sensors.txt'

print("Reading files ...")

sensors = np.genfromtxt(file_path, delimiter = ";", dtype = "str")


general_dict = {}
longitude = []
latitude = []
for sensor in sensors:
    general = sensor[0]
    general = general.split(",")
    # id = general[0]
    # general_dict[id] = general[1:]
    latitude.append(float(general[2]))
    longitude.append(float(general[3]))
# print longitude

# Set window position and zoom level
# Define map object: lat, lon, zoom level
mymap = pygmaps.pygmaps(52.07, 5.47, 8)
lons = longitude
lats = latitude
radius = 5
strokeColor = "#0000FF"
fillColor = "#0000FF"
fillOpacity = 0.3
strokeOpacity = 1.0
strokeWeight = 2
for i in range(len(lons)):
    x = lons[i]
    y = lats[i]
    mymap.addradpoint(y, x, radius, strokeColor, fillColor, fillOpacity, strokeOpacity, strokeWeight)

# Make html file (comparable to pygame.display.flip() or plt.show())
print "Showing map window..."
mymap.draw("test.html")

webbrowser.open("test.html")

print "Ready"

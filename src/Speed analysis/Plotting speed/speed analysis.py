import os
import numpy as np
import scipy.cluster.vq as clust
import pygmaps
import webbrowser


file_path_sensors = 'C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Traffic data\Traffic_speed data\Plotting traffic speed\Converted\sensors.txt'
# day = raw_input("Which day do you wanna analyze? (08-02-2017)")
day = "08-02-2017"
file_path_speed =  'C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Traffic data\Traffic_speed data\Plotting traffic speed' + "\\" + day + "\\"
times_speed = os.listdir(file_path_speed)
speed_documents = {}
for i in times_speed:
    key = i[:4]
    speed_documents[key] = i


print("Reading sensors files ...")
sensors = np.genfromtxt(file_path_sensors, delimiter = ";", dtype = "str")

sensor_dict = {}
sensor_tf = {}
for sensor in sensors:
    general = sensor[0]
    general = general.split(",")
    id = general[0]
    sensor_dict[id] = general[2:4]
    sensor_tf[id] = []
    for indeces in sensor[1:]:
        index = indeces.split(",")
        if index == [""]:
            continue
        elif index[2] == "trafficSpeed":
            sensor_dict[id].append(index[0])
        elif index[2] == "trafficFlow":
            sensor_tf[id] += [index[0]]

time_of_day = raw_input("What time of the day would you like to analyze? (hhmm)")

speed_file = file_path_speed + speed_documents[time_of_day]

longitude_lst = []
latitude_lst = []
speed_lst = []
name_sensors = []
missing_sensors = 0
good_sensors = 0
FUCKED_UP = 0
speed_of_sensors_dict = {}
with open(speed_file)as f:
        lines = f.readlines()
        for line in lines:
            try:
                line = line.split(";")
                id_speed = line[1]
                longitude = sensor_dict[id_speed][1]
                latitude = sensor_dict[id_speed][0]
                indices_speed = sensor_dict[id_speed][2:]
                indices_flow = sensor_tf[id_speed]

                average_speed_lst = []
                for i in indices_speed:
                    i = int(i)
                    if i+2>len(line):
                        break
                    traffic_speed = line[i+1].split(",")[1]
                    average_speed_lst.append(float(traffic_speed))
                flow_lst = []
                for j in indices_flow:
                    j = int(j)
                    if j+2>len(line):
                        break
                    traffic_flow = line[j+1].split(",")[0]
                    flow_lst.append(float(traffic_flow))
                total_cars = sum(flow_lst)
                percentages = map(lambda x: x/float(total_cars),flow_lst)

                if len(percentages) == len(average_speed_lst) + 1:
                    percentages = percentages[:-1]
                average = 0
                for k in range(len(percentages)):
                    if len(percentages) > len(average_speed_lst):
                        FUCKED_UP += 1
                        # print id_speed, percentages, average_speed_lst
                    percentage = percentages[k]
                    average_speed = average_speed_lst[k]
                    average += percentage*average_speed
                speed_lst.append(average)
                good_sensors += 1
                speed_of_sensors_dict[id_speed] = average
                longitude_lst.append(float(longitude))
                latitude_lst.append(float(latitude))
                name_sensors.append(id_speed)

            except:
                missing_sensors += 1
print good_sensors, missing_sensors

def rgb(minimum, maximum, value):
    minimum, maximum = float(minimum), float(maximum)
    ratio = 2 * (value - minimum) / (maximum - minimum)
    b = int(max(0, 255 * (ratio-1)))
    r = int(max(0, 255 * (1- ratio)))
    g = 255 - b - r
    return r, g, b


def converter(minimum, maximum, value):
    r, g, b = rgb(minimum, maximum, value)
    return '#%02x%02x%02x' % (r, g, b)


maximum = max(speed_lst)
minimum = min(speed_lst)
print maximum,minimum
# print len(longitude_lst), len(latitude_lst), len(speed_lst)

# # Set window position and zoom level
# # Define map object: lat, lon, zoom level
mymap = pygmaps.pygmaps(52.07, 5.47, 8)
lons = longitude_lst
lats = latitude_lst
radius = 600
fillOpacity = 0.3
strokeOpacity = 1.0
strokeWeight = 2
for i in range(len(lons)):
    x = lons[i]
    y = lats[i]
    if speed_lst[i] == 182.0:
        print name_sensors[i]
    # print type(x),type(y)
    color = converter(10,140,speed_lst[i])
    mymap.addradpoint(y, x, radius, color, color, fillOpacity, strokeOpacity, strokeWeight)

# Make html file (comparable to pygame.display.flip() or plt.show())
print "Showing map window..."
mymap.draw("test.html")

webbrowser.open("test.html")

print "Ready"

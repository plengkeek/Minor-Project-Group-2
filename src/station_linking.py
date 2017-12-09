import math

# Determine the average meter per degree longitude/latitude for the Netherlands
# this is are average values for latitude in the range of 50 to 54 degrees
# source: http://www.csgnetwork.com/degreelenllavcalc.html
meter_latitude_50_deg = 111229.02643399208
meter_latitude_54_deg = 111304.96143808351

meter_longitude_50_deg = 71695.72612886579
meter_longitude_54_deg = 65575.74805229454

avg_lat_meter = (meter_latitude_50_deg + meter_latitude_54_deg) / 2
avg_lon_meter = (meter_longitude_50_deg + meter_longitude_54_deg) / 2


# Store longitude, latitude and name data of the weather stations

with open("../example_data/KNMI_20171206.txt") as weather_file:
    data = weather_file.readlines()[5:55]

weather_stations = []

for entry in data:
    weather_dict = dict()

    weather_dict["longitude"] = float(entry[15:20])
    weather_dict["latitude"] = float(entry[23:33])
    weather_dict["name"] = entry[46:-1]
    weather_stations.append(weather_dict)


# Store longitude, latitude and id data of the traffic stations

with open("../example_data/sensors.txt") as traffic_file:
    data = traffic_file.readlines()

traffic_stations = []

for entry in data:
    traffic_dict = dict()
    values = entry.split(";")

    traffic_dict["id"] = values[0]
    traffic_dict["latitude"] = float(values[1])
    traffic_dict["longitude"] = float(values[2])
    traffic_stations.append(traffic_dict)


# Iterate over the traffic station and find the three closest weather stations to use for interpolation
for traffic_station in traffic_stations:
    # list of closest weather stations to this traffic station
    closest_stations = []

    for weather_station in weather_stations:
        # compute the distance from this weatherstation to this traffic station
        delta_longitude = traffic_station["longitude"] - weather_station["longitude"]
        delta_latitude = traffic_station["latitude"] - weather_station["latitude"]

        distance = math.sqrt((avg_lat_meter*delta_latitude)**2 + (avg_lon_meter*delta_longitude)**2)
        # store distance, is later used for weight computation
        weather_station["distance"] = distance

        # if there are less than three weather stations selected, just append it
        if len(closest_stations) < 3:
            closest_stations.append(weather_station)
        else:
            # if there is a closest station with a bigger distance than the weather station, replace closest station
            for station in closest_stations:
                if weather_station["distance"] < station["distance"]:
                    index = closest_stations.index(station)
                    closest_stations[index] = weather_station
                    break

    # compute the weights
    total_distance = 0
    for station in closest_stations:
        total_distance += station["distance"]

    # add traffic station to write string
    string_to_write = traffic_station["id"] + ";" + str(traffic_station["longitude"]) + ";" + \
                      str(traffic_station["latitude"]) + ";"

    # add weather stations to write string
    for station in closest_stations:
        weight = station["distance"] / total_distance
        string_to_write += station["name"] + ";" + str(weight) + ";" + str(station["longitude"]) + ";" + \
                           str(station["latitude"]) + ";"

    string_to_write += '\n'

    with open("../example_data/linked_station.txt", 'a') as file:
        file.write(string_to_write)

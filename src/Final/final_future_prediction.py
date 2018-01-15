import numpy as np
import os
import pygmaps
import webbrowser

def congestion_analysis(Q2):
    LA_folder = "C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Final complete process\MLA output"
    file_average_speeds = "C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Final complete process\Statistical analysis\\speeds_corrected_pure.txt"
    locations_file = "C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Final complete process\Statistical analysis\\lat_and_lon.txt"

    locations = np.genfromtxt(locations_file, delimiter = ";", dtype = str)

    lon_dict = {}
    lat_dict = {}
    for i in range(len(locations)):
        lon_dict[locations[i,0]] = float(locations[i,2])
        lat_dict[locations[i,0]] = float(locations[i,1])


    threshold_tj = 0.4
    prediction_time = 15
    max_time = 15.

    queue = Q2.get()
    print queue
    # directory = os.listdir(LA_folder)[:-1]
    LA_file = LA_folder + "\\" + queue[1]

    LA_content = np.genfromtxt(LA_file, delimiter = ",", dtype = str)
    average_speeds_content = np.genfromtxt(file_average_speeds, delimiter = "  ", dtype = str)

    average_speeds_dict = {}
    for i in range(len(average_speeds_content)):
        average_speeds_dict[average_speeds_content[i,0]] = threshold_tj * float(average_speeds_content[i,1])

    LA_dict = {}
    for j in range(len(LA_content)):
        LA_dict[LA_content[j,0]] = float(LA_content[j,1])

    sensors = average_speeds_dict.keys()
    decision = {x : "" for x in sensors}

    # Faking the existence of a dictionary with all the speeds
    slope_dict = {x : [] for x in sensors}
    for file in queue[1:]:
        temp = {}
        content = np.genfromtxt(LA_folder + "\\" + file, delimiter = ",", dtype = str)
        for j in range(len(content)):
            temp[content[j, 0]] = float(content[j, 1])
        for sensor in sensors:
            speed = temp[sensor]
            slope_dict[sensor].append(speed)

    probability_dict = {}
    for sensor in sensors:
        if LA_dict[sensor] <= average_speeds_dict[sensor]:
            decision[sensor] = prediction_time
            probability_dict[sensor] = 1
        elif slope_dict[sensor][0]>=slope_dict[sensor][1]>=slope_dict[sensor][2]>=slope_dict[sensor][3]:
            slope_lst = []
            for data_pairs in range(len(slope_dict[sensor])-1):
                slope = slope_dict[sensor][data_pairs+1] - slope_dict[sensor][data_pairs]
                slope_lst.append(slope)
            final_slope = float(sum(slope_lst))/len(slope_lst)
            time_needed = int((slope_dict[sensor][-1] - average_speeds_dict[sensor])/abs(final_slope)) + 1
            if time_needed>= max_time:
                decision[sensor] = [-1]
                probability_dict[sensor] = 0
            else:
                first_probability = (max_time - time_needed)/max_time

                xlst = np.asarray(range(len(queue[1:])))
                ylst = np.asarray(slope_dict[sensor])
                coeffs = np.polyfit(xlst,ylst,1)
                p = np.poly1d(coeffs)
                yhat = p(xlst)
                ybar = np.sum(ylst)/len(ylst)
                ssreg = np.sum((yhat - ybar) ** 2)
                sstot = np.sum((ylst - ybar) ** 2)

                R2 = ssreg / sstot
                final_probability = first_probability*R2
                print sensor, time_needed, final_slope, first_probability, R2, slope_dict[sensor], final_probability
                decision[sensor] = prediction_time + time_needed
                probability_dict[sensor] = round(final_probability,4)
        else:
            decision[sensor] = [-1]
            probability_dict[sensor] = 0

    # Functions for the plotting in Google Maps

    def rgb(minimum, maximum, value, prob):
        minimum, maximum = float(minimum), float(maximum)
        ratio = 2 * (value - minimum) / (maximum - minimum)
        if not prob:
            r = int(max(0, 255 * (1 - ratio)))
            g = int(max(0, 255 * (ratio - 1)))
            b = 255 - g - r
        else:
            g = int(max(0, 255 * (1 - ratio)))
            r = int(max(0, 255 * (ratio - 1)))
            b = 255 - g - r
        return r, g, b

    def converter(minimum, maximum, value, prob):
        r, g, b = rgb(minimum, maximum, value, prob)
        return '#%02x%02x%02x' % (r, g, b)


    # # Set window position and zoom level
    # # Define map object: lat, lon, zoom level
    mymap = pygmaps.pygmaps(52.07, 5.47, 8)
    radius = 600
    fillOpacity = 0.3
    strokeOpacity = 1.0
    strokeWeight = 2
    for i in range(len(sensors)):
        x = lon_dict[sensors[i]]
        y = lat_dict[sensors[i]]
        value = LA_dict[sensors[i]]
        color = converter(10,140,value, False)
        mymap.addradpoint(y, x, radius, color, color, fillOpacity, strokeOpacity, strokeWeight)

    # Make html file (comparable to pygame.display.flip() or plt.show())
    print "Showing map window..."
    mymap.draw("speeds.html")

    webbrowser.open("speeds.html")

    print "Ready"

    # # Set window position and zoom level
    # # Define map object: lat, lon, zoom level
    mymap = pygmaps.pygmaps(52.07, 5.47, 8)
    radius = 600
    fillOpacity = 0.3
    strokeOpacity = 1.0
    strokeWeight = 2
    for i in range(len(sensors)):
        x = lon_dict[sensors[i]]
        y = lat_dict[sensors[i]]
        value = probability_dict[sensors[i]]
        color = converter(0,1,value, True)
        mymap.addradpoint(y, x, radius, color, color, fillOpacity, strokeOpacity, strokeWeight)

    # Make html file (comparable to pygame.display.flip() or plt.show())
    print "Showing map window..."
    mymap.draw("prob.html")

    webbrowser.open("prob.html")

    print "Ready"

    last_element = queue.pop()
    Q2.put(queue)
    os.remove(LA_folder + "\\" + last_element)

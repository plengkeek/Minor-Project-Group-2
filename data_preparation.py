# coding: utf-8

# This notebook uses two implicitly defined variables, namely: sc and spark. These are used to work with SPARK RDD and SPARK DataFrame API

# In[79]:
import pandas
import pyspark.sql as sql
from pyspark.sql.types import StringType, FloatType, StructType, StructField, IntegerType

# Specify the paths where the various data files can be found.
# - traffic_path: path that points to the folder with the text files that contain the measurements (extracted from the XML files provided by the NDW)
# - weather_path: path that point to the folder with text files containing the weather measurements from the KNMI
# - linked_path: path to a text file that lists the three closest weather stations for every traffic camera
# - speed_path: text that specifies wich indices of the text file from `traffic_path` should be used to compute a weighted average
# - flow_path: similar to `speed_path`, only for the traffic flow values

# In[80]:


def data_prep(traffic_path="/home/thijs-gerrit/Desktop/p01-01-2017/*.txt",
              weather_path="/home/thijs-gerrit/Documents/KNMI/*",
              linked_path="/home/thijs-gerrit/Documents/station.txt",
              speed_path="/home/thijs-gerrit/Documents/trafficspeed.txt",
              flow_path="/home/thijs-gerrit/Documents/trafficflow.txt"):

    # Define functions that split the data. These are made so that for every line only a single split has to be performed instead of multiple

    # In[81]:

    def id_split(x):
        temp = x.split(',')
        return (temp[0], temp[1:])

    # In[83]:

    def split_traffic_data(x):
        temp = x.split(";")
        return (temp[1], [temp[0]] + temp[2:-1])

    # load the files from `speed_path` and `flow_path` and join them

    # In[82]:

    speed = sc.textFile(speed_path).map(lambda x: id_split(x))
    flow = sc.textFile(flow_path).map(lambda x: id_split(x))
    spd_flw_indices = flow.join(speed)

    # load the data from the text files coming from `traffic_path`

    # In[84]:

    traffic_data = sc.textFile(traffic_path).map(
        lambda x: split_traffic_data(x))

    # # Compute the average speed per location and timestamp
    #

    # this function computes the weighted average speed which will function as the target for the machine learning model

    # In[3]:

    def avg_speed(x):
        """
        Compute a weighted average of the traffic speed
        """
        spd_indices = list(map(int, x[1][0][1]))
        flw_indices = list(map(int, x[1][0][0]))

        flw_vals = []

        if len(flw_indices) > 0:
            for index in flw_indices:
                try:
                    flw_val = float(x[1][1][index].split(",")[0])
                except IndexError:
                    return -2.0

                if flw_val < 0:
                    flw_vals.append(0.0)
                else:
                    flw_vals.append(flw_val)

        total_flw = sum(flw_vals)

        if total_flw > 0:
            weights = []

            for val in flw_vals:
                weights.append(val / total_flw)

            avg_spd = 0
            if len(spd_indices) > 0:
                for i in range(len(spd_indices)):
                    index = spd_indices[i]
                    spd_val = float(x[1][1][index].split(",")[1])

                    #                 if index+1 < len(spd_indices):
                    if i + 1 <= len(spd_indices):
                        avg_spd += spd_val * weights[i]

            return float(avg_spd)

        else:
            return -1.0

    # In[87]:

    def timestamp(x):
        """
        Extract the timestamp from a string
        """
        time_str = x[1][1][0]

        time_temp = time_str.split("T")

        date = time_temp[0].split("-")
        time = time_temp[1].split(":")

        timestamp = date + time[:-1]  #exclude seconds in the timestamp

        return list(map(int, timestamp))

    # In[1]:

    def speed_ID_pair(x):
        """
        Pair the ID with the average speed and timestamp
        """
        return [x[0]] + timestamp(x) + [avg_speed(x)]

    # In[89]:

    average_speeds = spd_flw_indices.join(traffic_data).map(
        lambda x: speed_ID_pair(x))

    # convert the `average_speeds` from an rdd to a DataFrame, but first create a schema (or structure) for it

    # In[90]:

    avg_speed_struct = StructType([
        StructField('ID', StringType(), False),
        StructField('Year', IntegerType(), False),
        StructField('Month', IntegerType(), False),
        StructField('Day', IntegerType(), False),
        StructField('Hour', IntegerType(), False),
        StructField('Minute', IntegerType(), False),
        StructField('AvgSpeed', FloatType(), False)
    ])

    # In[91]:

    avg_speed_frame = spark.createDataFrame(average_speeds, avg_speed_struct)

    # Create the structures for the different DataFrames. These structures specify the column names, data type and if the column is allowed to empty (nullable)

    # In[92]:

    # create structure for the traffic speed DataFrame
    speed_struct = StructType([StructField("ID", StringType(), False)])

    for i in range(7):
        speed_struct.add("Index{0}".format(i + 1), IntegerType(), False)

    # In[93]:

    # create structure for the traffic flow DataFrame
    flow_struct = StructType([StructField("ID", StringType(), False)])

    for i in range(7):
        flow_struct.add("Index{0}".format(i + 1), IntegerType(), False)

    # In[94]:

    # read traffic speed file
    speed_frame = spark.read.csv(path=speed_path, schema=speed_struct, sep=",")

    # read traffic flow file
    flow_frame = spark.read.csv(path=flow_path, schema=flow_struct, sep=",")

    # In[95]:

    # create structure for the weather DataFrame
    weather_struct = StructType([
        StructField("STN", IntegerType(), False),
        StructField("Year", IntegerType(), False),
        StructField("Month", IntegerType(), False),
        StructField("Day", IntegerType(), False),
        StructField("FG", FloatType(), True),
        StructField("DR", FloatType(), True),
        StructField("RH", FloatType(), True),
        StructField("VVN", FloatType(), True),
        StructField("VVX", FloatType(), True)
    ])

    # In[96]:

    # create structure for the linked station DataFrame
    linked_struct = StructType([
        StructField("TrafficID", StringType(), False),
        StructField("TrafficLON", FloatType(), False),
        StructField("TrafficLAT", FloatType(), False),
    ])

    for i in range(3):
        linked_struct.add("W{0}STN".format(i + 1), IntegerType(), False)
        linked_struct.add("W{0}Weight".format(i + 1), FloatType(), False)
        linked_struct.add("W{0}LON".format(i + 1), FloatType(), False)
        linked_struct.add("W{0}LAT".format(i + 1), FloatType(), False)

    # Next up, the data files need to be loaded into DataFrames. Here we specify which structure (or schema) is expected and what delimter is used in the data files

    # In[97]:

    # read weather files
    weather_frame = spark.read.csv(
        path=weather_path, schema=weather_struct, sep=";")
    # read linked stations file
    linked_frame = spark.read.csv(
        path=linked_path, schema=linked_struct, sep=";")

    # We now register the DataFrames as table's, so SQL queries can be used

    # In[98]:

    avg_speed_frame.createOrReplaceTempView("Speed")
    weather_frame.createOrReplaceTempView("Weather")
    linked_frame.createOrReplaceTempView("LinkedStations")

    # Here an SQL Query is used to join the tables and filter out the rows that are not usable

    # In[99]:

    query = """
        SELECT L.TrafficID AS ID,
            S.AvgSpeed AS AvgSpeed,
            (L.W1Weight * W1.FG + L.W2Weight * W2.FG + L.W3Weight * W3.FG) / 3 AS FG,
            (L.W1Weight * W1.DR + L.W2Weight * W2.DR + L.W3Weight * W3.DR) / 3 AS DR,
            (L.W1Weight * W1.RH + L.W2Weight * W2.RH + L.W3Weight * W3.RH) / 3 AS RH,
            (L.W1Weight * W1.VVN + L.W2Weight * W2.VVN + L.W3Weight * W3.VVN) / 3 AS VVN,
            (L.W1Weight * W1.VVX + L.W2Weight * W2.VVX + L.W3Weight * W3.VVX) / 3 AS VVX
        FROM LinkedStations AS L
        INNER JOIN Weather AS W1
        ON L.W1STN = W1.STN
        INNER JOIN Weather AS W2
        ON L.W2STN = W2.STN
        INNER JOIN Weather AS W3
        ON L.W3STN = W3.STN
        INNER JOIN Speed AS S
        ON L.TrafficID = S.ID
        WHERE S.Year = W1.Year AND S.Year = W2.Year AND S.Year = W3.Year AND
            S.Month = W1.Month AND S.Month = W2.Month AND S.Month = W3.Month AND
            S.Day = W1.Day AND S.Day = W2.Day AND S.Day = W3.Day AND
            W1.FG IS NOT NULL  AND W2.FG IS NOT NULL AND W3.FG IS NOT NULL AND
            W1.DR IS NOT NULL  AND W2.DR IS NOT NULL AND W3.DR IS NOT NULL AND
            W1.RH IS NOT NULL  AND W2.RH IS NOT NULL AND W3.RH IS NOT NULL AND
            W1.VVN IS NOT NULL  AND W2.VVN IS NOT NULL AND W3.VVN IS NOT NULL AND
            W1.VVX IS NOT NULL  AND W2.VVX IS NOT NULL AND W3.VVX IS NOT NULL AND
            S.AvgSpeed > -1
        """

    # In[100]:

    panda_df = spark.sql(query).toPandas()
    ids = panda_df.iloc[:, 0]
    values = panda_df.iloc[:, 1:]
    return [values, ids]

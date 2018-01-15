# NOTE:
# Data geselecteerd vanaf KNMI site:
# FG, DR, RH, VVN, VVX


def process_knmi(file, output_path):
    """Convert a KNMI weather data text file to a PySpark DataFrame"""

    def change_format(x):
        """This function converts a line of KNMI weather data to a list format
        for use in a RDD"""
        # convert it to a more traditional csv format en split it
        x = x.replace(" ", "")
        x = x.split(",")

        # split the date stamp in seperate values for year, month and day
        date = x[1]
        year = date[:4]
        month = date[4:6]
        day = date[6:8]

        new_line = [x[0]] + [year, month, day] + x[2:]

        # return the new format
        return ";".join(new_line)

    rdd = sc.textFile(file)

    # filter out the correct data
    rdd = rdd.filter(lambda x: "#" not in x)\
                .map(lambda x: change_format(x))\
                .saveAsTextFile(output_path)

    print("Done")

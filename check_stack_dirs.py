import webdav.client as wc
from stack_connect import connect
import sys

# connect to the STACK server
client = connect()

# check which files are in the STACK directory
stack_files = client.list("historicaldata/intensityandspeed")

# read the already processed files from the text file
with open("finished_files.txt") as file:
    finished_files = []

    # read all the lines
    while True:
        line = file.readline()[:-1]

        # check if the end of the file has been reached
        if line == "":
            break

        # append the file
        finished_files.append(line)

# determine which files are not yet processed
to_process = set(stack_files) - set(finished_files)
to_process = list(to_process)

# convert the list to a comma seperated file. This way it can be printed to the
# command line
to_process = "\n".join(to_process)

# write results to command line
with open("to_download.txt", "w") as file:
    file.write(to_process)

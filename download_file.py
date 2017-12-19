"""
A python script to download a file from a STACK server.

Python 2.7 script. When called from the command line, it should have this
format (on Linux): $ pyhton2 download_file.py <file_to_download>
"""

from stack_connect import connect
import sys

# connect client to the server
client = connect()

# from the command line, what file needs to be downloaded?
file = sys.argv[1]

client.download_sync(
    remote_path="historicaldata/intensityandspeed/" + file,
    local_path="_cache/" + file)

sys.exit(0)

import webdav.client as wc


def connect(file="config.txt"):
    """
    Load the configurations from the text file and set them up for use.
    This file is Python 2 & 3 compatible
    """
    # create an empty dict with stack connection options
    options = dict()

    # read the connection options from the config file
    with open("config.txt") as file:
        config = file.readlines()

    # add the settings to the options dictionary
    for setting in config:
        setting = setting.split("=")
        options[setting[0]] = setting[1][:-1]

    return wc.Client(options)

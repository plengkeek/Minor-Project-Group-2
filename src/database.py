import pymysql

'''
Database login data:
host = johnny.heliohost.org
port = 3306
user = minor_read
passwd = goingBIG
db = minor_static_data
'''

class Database:
    def __init__(self):
        self.__is_connected = False
        self.__connection = None

    def connect(self):
        try:
            self.__connection = pymysql.connect(host='johnny.heliohost.org', port=3306, user='minor_read',
                                                passwd='goingBIG', db='minor_static_data')
            self.__is_connected = True
            print("Connection to SQL database successful")

        except pymysql.err.OperationalError as exc:
            self.__is_connected = False
            print("There could be no connection established with the database")
            print('Error message: ' + str(exc.args[0]) + ' ' + str(exc.args[1]))

    def close_connection(self):
        self.__connection.close()
        self.__is_connected = False
        
db = Database()
db.connect()

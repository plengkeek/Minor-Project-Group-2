import pymysql

conn = pymysql.connect(host='johnny.heliohost.org', port=3306, user='minor_read', passwd='goingBIG', db='minor_static_data')
conn.close()

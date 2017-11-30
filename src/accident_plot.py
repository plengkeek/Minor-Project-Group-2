import os
import numpy as np
from incident_converter import IncidentConverter

file_path = 'F:\Minor project DATA\Converted\\'
XML_path = 'F:\Minor project DATA\Accidents\\'

files = os.listdir(file_path)
XMLfiles = os.listdir(XML_path)

# for file in XML_files:
#     IncidentConverter(XML_path+file)
# print (files)

for file in files[:200]:
    file_info = os.stat(file_path+file)
    if file_info.st_size == 0:
        os.remove(file_path+file)
        index_to_delete = files.index(file_path+file)
        del files[index_to_delete]

crashes = np.genfromtxt(file_path+files[0],delimiter=';',dtype='str')
longitud = len(crashes)
for i in range(1,len(files[:200])):
    data = np.genfromtxt(file_path+files[i],delimiter=';',dtype='str')
    longitud = longitud + len(data)
    crashes = np.vstack((crashes,data))

final = np.unique(crashes,axis=0)
result = np.sort(final[:,0])
# print(len(result))
# print (longitud)
# # print(final)

time = final[:,0]
latitude = final[:,1]
longitude = final[:,2]
type = final[:,3]

import matplotlib.pyplot as plt
plt.plot(longitude,latitude,'ro')
plt.show()
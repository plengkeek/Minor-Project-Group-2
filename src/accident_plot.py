import os
import numpy as np
import scipy.cluster.vq as clust
import matplotlib.pyplot as plt
from incident_converter import IncidentConverter

k = int(input("Enter the number of clusters to create: [1-100]"))
input("System ready. Press ENTER to start")

file_path = 'F:\Minor project DATA\Converted\\'
XML_path = 'F:\Minor project DATA\Accidents\\'

files = os.listdir(file_path)
XMLfiles = os.listdir(XML_path)

print("Reading files ...")
# for file in XML_files:
#     IncidentConverter(XML_path+file)
# print (files)

for file in files:
    file_info = os.stat(file_path+file)
    if file_info.st_size == 0:
        os.remove(file_path+file)
        index_to_delete = files.index(file_path+file)
        del files[index_to_delete]

crashes = np.genfromtxt(file_path+files[0],delimiter=';',dtype='str')
longitud = len(crashes)
for i in range(1,len(files)):
    progress = (i/len(files))*100
    if progress%5 < 0.013:
        print("Progress is "+str(np.floor(progress))+" %")
        continue

    data = np.genfromtxt(file_path+files[i],delimiter=';',dtype='str')
    longitud = longitud + len(data)
    crashes = np.vstack((crashes,data))

del files
del XMLfiles


final = np.unique(crashes,axis=0)
del crashes
result = np.sort(final[:,0])
# print(len(result))
# print (longitud)
# # print(final)

print("Processing results ... ")
time = final[:,0]
latitude = (final[:,1])
longitude = (final[:,2])
type = final[:,3]


coordinates = np.zeros((len(latitude),2))
for lat in range(len(latitude)):
    coordinates[lat,0] = float(latitude[lat])
    coordinates[lat,1] = float(longitude[lat])

# print(coordinates)
critical = np.zeros((len(latitude),3))
critical = clust.kmeans2(coordinates,k,minit='points')
cluster_numbers = critical[1].reshape((len(latitude),1))
clusters = np.hstack((coordinates,cluster_numbers))
cluster_size = []


groups = k*[[]]

for i in range(len(clusters[:,2])):
    if groups[int(clusters[:,2][i])] == []:
        groups[int(clusters[:, 2][i])] = [[clusters[i,0],clusters[i,1],clusters[i,2]]]
    else:
        groups[int(clusters[:,2][i])].append([clusters[i,0],clusters[i,1],clusters[i,2]])

del clusters


cluster_array = np.array(groups)
for i in groups:
    cluster_size.append(len(i))
del groups

print(''' ------------- RESULTS ------------- 
In the considered time period, there have been '''+str(len(coordinates))+''' crashes.

... showing plot of main critical areas ...''')

plt.plot(critical[0][:,1],critical[0][:,0],'ro')
plt.show()
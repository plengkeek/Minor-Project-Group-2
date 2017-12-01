import os
import numpy as np
import scipy.cluster.vq as clust
import matplotlib.pyplot as plt
from incident_converter import IncidentConverter
from mpl_toolkits.basemap import Basemap
import matplotlib as mpl
import matplotlib.cm as cm

k = int(input("Enter the number of clusters to create: [1-100]"))
# raw_input("System ready. Press ENTER to start")

file_path = 'C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Converted\\'
# XML_path = 'D:\Minor project DATA\Accidents\\'

files = os.listdir(file_path)
# XMLfiles = os.listdir(XML_path)

print("Reading files ...")
# for file in XML_files:
#     IncidentConverter(XML_path+file)
# print (files)
n_files = files[:]
for file in n_files:
    file_info = os.stat(file_path+file)
    if file_info.st_size == 0:
        os.remove(file_path+file)
        index_to_delete = files.index(file_path+file)
        del files[index_to_delete]

crashes = np.genfromtxt(file_path+files[0],delimiter=';',dtype='str')

print len(files)
for i in range(1,len(n_files)):
    progress = (float(i)/len(n_files))*100
    if progress%5 < 0.013:
        print("Progress is "+str(np.floor(progress))+" %")
        continue

    data = np.genfromtxt(file_path+files[i],delimiter=';',dtype='str')
    crashes = np.vstack((crashes,data))

del files
# del XMLfiles


final = np.unique(crashes,axis=0)
del crashes
result = np.sort(final[:,0])


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
#
# print(''' ------------- RESULTS -------------
# In the considered time period, there have been '''+str(len(coordinates))+''' crashes.
#
# ... showing plot of main critical areas ...''')

# plt.plot(critical[0][:,1],critical[0][:,0],'ro')
# plt.show()

map = Basemap(projection='merc', lat_0=1.65, lon_0=52.10,
              resolution = 'f', area_thresh = 0.000001,
                         llcrnrlon = 3.2, llcrnrlat = 50.5,
                                                          urcrnrlon = 7.3, urcrnrlat = 53.8)

map.drawcoastlines(color = "darkblue")
map.drawcountries(color='k')
map.fillcontinents(color='wheat')
map.drawmapboundary()


lons = coordinates[:,1]
lats = coordinates[:,0]
x, y = map(lons, lats)
map.plot(x, y, 'ro', markersize=2)

#######################################################################################################################
# # Floats between 0 and 20
# indexList = cluster_size
# minIndex = min(indexList)
# maxIndex = max(indexList)
#
#
# # Figure out colors for each index
# norm = mpl.colors.Normalize(vmin=minIndex, vmax=maxIndex, clip=True)
# mapper = cm.ScalarMappable(norm=norm, cmap='RdYlBu')
#
# gridIndexDict = []
# for key in indexList:
#     value = mapper.to_rgba(key)
#     gridIndexDict.append(value)
#
# new_colors = []
# for j in range(len(indexList)):
#     color = gridIndexDict[j]
#     rgb1 = color[0]
#     rgb2 = color[1]
#     rgb3 = color[2]
#     new_color = (rgb1,rgb2,rgb3)
#     new_colors.append(new_color)
# mapper.set_array(indexList)
# plt.colorbar(mapper)
#######################################################################################################################


for crash in range(len(critical[0])):
    size = (float(cluster_size[crash])/max(cluster_size))
    x,y =map(critical[0][crash,1],critical[0][crash,0])
    map.plot(x,y,'go',markersize=size*25, alpha = 0.5)

plt.show()
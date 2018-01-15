import os
from glob import glob
import numpy as np
import keras.layers as L
import keras.models as M
from keras import callbacks
import matplotlib.pyplot as plt

# Load csv files
files = glob('/run/media/plengkeek/Trafficdata/STACK/dataframe/test/*')
arrays = [np.genfromtxt(f, delimiter=',', skip_header=1, dtype=str) for f in files]
dataframe = np.concatenate(arrays)

# sort the array
dataframe = dataframe[np.lexsort((dataframe[:,3].astype(int), dataframe[:,4].astype(int), dataframe[:,5].astype(int), dataframe[:,6].astype(int), dataframe[:,7].astype(int)))]

# Extract a single sensor
singlelocdataframe = []
for line in dataframe:
    if line[0] == 'GEO03_D4T-RWS_I_506_V_0017_ID_1088':
        singlelocdataframe.append(line[7:].astype(float).tolist())

# Setup the train_x, train_y
data_x = np.zeros((int(len(singlelocdataframe) / 2), 2, 7))
data_y = np.zeros((int(len(singlelocdataframe) / 2), 2, 7))

# Create to training set with 2 timesteps
timesteps = 2
for i in range(len(singlelocdataframe) // 2):
    data_x[i] = singlelocdataframe[i:timesteps + i]
    data_y[i] = singlelocdataframe[i + 1:timesteps + i + 1]

# Normalize
scale_x = np.max(data_x)
scale_y = np.max(data_y)

data_x = data_x / scale_x
data_y = data_y / scale_y

# Create the model
model_input = L.Input(shape=(2, 7))
layer1 = L.LSTM(32, return_sequences=True)(model_input)
layer2 = L.LSTM(32, return_sequences=True)(layer1)
model_output = L.LSTM(7, return_sequences=True)(layer2)

model = M.Model(inputs=model_input, outputs=model_output)

model.compile(loss='mse', optimizer='adam', metrics=['accuracy'])

# Train the model
history = model.fit(data_x, data_y, validation_split=0.33, epochs=50)
'''''
,
          callbacks=[callbacks.EarlyStopping(monitor='val_acc',
                              min_delta=0.01,
                              patience=2,
                              verbose=0, mode='max')])
'''''

# Make a prediction
print(model.predict(data_x[0:2]) * scale_y)

# Plot the accuracy and error
plt.plot(history.history['acc'])
plt.plot(history.history['val_acc'])
plt.title('model accuracy')
plt.ylabel('accuracy')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='upper left')
plt.show()
plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.title('model loss')
plt.ylabel('loss')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='upper left')
plt.show()
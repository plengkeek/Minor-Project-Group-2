import numpy as np
import io
import keras.layers as L
import keras.models as M
from keras import callbacks

data = io.BytesIO(open('data.txt', 'rb').read().replace(b';', b','))
data = np.genfromtxt(data, delimiter=',')
data = np.delete(data, np.s_[0:2], axis=1)
data = np.delete(data, np.s_[-1], axis=1)

data_combined = []
for i in range(len(data)-5):
    data_combined.append(data[i:i+5].tolist())
data_combined = np.array(data_combined)

data_x = []
data_y = []
for i in range(len(data_combined)-1):
    data_x.append(data_combined[i])
    data_y.append(data_combined[i+1])
data_x = np.array(data_x)
data_y = np.array(data_y)

scale_x = np.max(data_x)
scale_y = np.max(data_y)

data_x = data_x / scale_x
data_y = data_y / scale_y


model_input = L.Input(shape=(5, 32))
layer1 = L.LSTM(32, return_sequences=True)(model_input)
model_output = L.LSTM(32, return_sequences=True)(layer1)

model = M.Model(inputs=model_input, outputs=model_output)

model.compile(optimizer='rmsprop', loss='mean_squared_logarithmic_error', metrics=['accuracy'])

model.fit(data_x, data_y, validation_split=0.33, epochs=50,
          callbacks=[callbacks.EarlyStopping(monitor='val_acc',
                              min_delta=0.01,
                              patience=2,
                              verbose=0, mode='max')])

print(model.predict(data_x[0:1]) * scale_y)

from keras.models import load_model
import datetime

def machine_learning(Q2, Q3):
    model = load_model('first_model.h5')

    dataframe = Q2.get()
    input = dataframe[0]
    output = model.prediction(input)

    names = dataframe[1]
    now = datetime.datetime.now()
    f = open("MLA output\\" + str(now) + ".txt", "w")
    for i in names:
        text = str(names[i]) + "," + output[0][i][0]
        f.write(text)

    f.close()

    current = Q3.get()
    new = current.append(str(now) + ".txt")
    Q3.put(new)





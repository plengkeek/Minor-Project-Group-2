from final_future_prediction import *
from ftp_stream import *
from knmi_stream import *
from queue import *
from machine_learning_algorithm import *
import threading
import time



if __name__ == "__main__":
    Q1 = queue()
    Q2 = queue()
    Q3 = queue()


    while True:
        # Check if there is something to download
        stream = FTPStream(Q1)
        weather_stream = KNMIStream()
        stream.start()
        weather_stream.start()

        # If there is nothing in the first queue, then wait and restart the process
        if Q1.size() == 0:
            time.sleep(25)
            continue

        else:


            # If there is something in the queue, then run the machine learning algorithm and the statistical analysis
            t2 = threading.Thread(target = machine_learning, args = (Q2, Q3))
            t2.start()
            t2.join()

            # Once the machine learning algorithm is finished, then carry out the congestion statistical analysis
            t3 = threading.Thread(target = congestion_analysis, args = (Q3,))
            t3.start()
            t3.join()








# Q2 = Queue()
# files = os.listdir("C:\Users\TUDelft SID\Documents\\2017-2018\\2nd quarter\Minorproject software design and application\Final complete process\MLA output")
# Q2.put(files)
#
# t3 = threading.Thread(target = congestion_analysis, args = (Q2,))
# t3.start()
# t3.join()
#
# while Q2.empty() is False:
#     print Q2.get()
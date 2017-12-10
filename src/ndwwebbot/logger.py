from threading import Thread
import os, time


class Logger(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        try:
            os.remove('log.txt')
        except Exception as e:
            print(e)

    def run(self):
        while True:
            if len(self.queue) != 0:
                identity, t, message = self.queue.popleft()
                with open('log.txt', 'a') as log:
                    log.write('Thread' + str(identity) + ' ' + t + ' ' + message + '\n')
                    log.flush()

                # Clear the console
                os.system('cls' if os.name == 'nt' else 'clear')
                with open('log.txt', 'r') as log:
                    lines = log.readlines()
                print('|---------------------------------------------------------------|')
                for line in lines[-20:]:
                    print("| " + line[:-1])
            else:
                time.sleep(1)



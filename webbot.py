from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from urllib import request
import time, os
from datetime import datetime, timedelta
from twocaptchaapi import TwoCaptchaApi
from PIL import Image
from threading import Thread
from queue import Queue
import easywebdav as wd
import random

download_q = Queue()
upload_q = Queue()

start = datetime.strptime("9-2-2017", "%d-%m-%Y")
stop = datetime.strptime("30-11-2017", "%d-%m-%Y")

download_q.put(('historicaldata/intensityandspeed', 'Intensiteiten en snelheden', '31-1-2017', '31-1-2017'))
download_q.put(('historicaldata/intensityandspeed', 'Intensiteiten en snelheden', '2-02-2017', '2-02-2017'))
while start < stop:
    download_q.put(('historicaldata/intensityandspeed', 'Intensiteiten en snelheden', start.strftime("%d-%m-%Y"),
                    start.strftime("%d-%m-%Y")))
    download_q.put(('historicaldata/traveltime', 'Reistijden', start.strftime("%d-%m-%Y"), start.strftime("%d-%m-%Y")))
    start = start + timedelta(days=1)


class STACKUploader(Thread):
    def __init__(self, id):
        Thread.__init__(self)
        self.stack = None
        self.id = id

    def __connect(self):
        self.stack = wd.connect(host="WEBADRESS", protocol="https", verify_ssl=True,
                                username='ACCOUNTNAME', password='PASSWORD')

    def upload(self, folder, file):
        self.stack.upload(file, "/remote.php/webdav/" + folder + '/' + file)

    def run(self):
        time.sleep(random.random())
        while True:
            self.__connect()
            if not upload_q.empty():
                folder, file = upload_q.get()
                print('Uploading ' + file + '...')
                self.upload(folder, file)
                print(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
                  + " Thread: " + str(self.id) + ' Finished Uploading ' + file + '...')
                os.remove(file)
            time.sleep(30)


class NDWWebBot(Thread):
    def __init__(self, id):
        Thread.__init__(self)
        self.id = id
        self.start_time = 0
        self.browser = webdriver.Chrome("/home/pleng/Desktop/chromedriver")

    def __open_browser(self):
        self.browser.get(('http://83.247.110.3/OpenDataHistorie'))
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        self.datatype = self.browser.find_element_by_id("productId")
        self.start_date = self.browser.find_element_by_id("fromDate")
        self.end_date = self.browser.find_element_by_id("untilDate")
        self.end_time = self.browser.find_element_by_id("untilTime")

    def __fill_form(self, data_type, start_date, end_data):
        self.datatype.send_keys(data_type)
        self.start_date.send_keys(start_date)
        self.end_date.send_keys(end_data)
        self.end_time.click()

    def __solve_captcha(self):
        # Save the captcha to file
        img = self.browser.find_element_by_id('CaptchaImage')

        location, size = img.location_once_scrolled_into_view, img.size
        self.browser.save_screenshot('screenshot' + str(self.id) + '.png')
        im = Image.open('screenshot' + str(self.id) + '.png')

        src = img.get_attribute('src')
        request.urlretrieve(src, 'captcha' + str(self.id) + '.png')

        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']

        im = im.crop((left, top, right, bottom))
        im.save('captcha' + str(self.id) + '.png')

        api = TwoCaptchaApi('b51dc904b4f0afc3693977440d8e2e02')

        answer = None
        while answer is None:
            try:
                with open('captcha' + str(self.id) + '.png', 'rb') as captcha_file:
                    captcha = api.solve(captcha_file)
                answer = captcha.await_result()
            except:
                answer = None
                continue

        if len(answer) > 2:
            answer = self.__solve_captcha()
        return answer

    def reporthook(self, count, block_size, total_size):
        if count == 0:
            self.start_time = time.time()
            return
        duration = time.time() - self.start_time
        progress_size = int(count * block_size)
        total = total_size / (1024 * 1024)
        transfer_rate = (float(count * block_size) / 1000) / duration  # kbytes/s

        if duration > 7200:
            raise Exception('Download took too long')

        if count % 1000 == 0:
            print(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
                  + " Thread: " + str(self.id) + " ...%d/%d MB, %d KB/s, %d seconds passed" %
                  (progress_size / (1024 * 1024), total, transfer_rate, duration))

    def run(self):
        time.sleep(random.random())
        while not download_q.empty():
            self.__open_browser()
            folder, data_type, start_date, end_data = download_q.get()
            print(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
                  + " Thread: " + str(self.id) + ' ' + data_type, start_date, end_data)
            self.__fill_form(data_type, start_date, end_data)

            print(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
                  + " Thread: " + str(self.id) + ' Solving Captcha...')
            result = self.__solve_captcha()
            print(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
                  + " Thread: " + str(self.id) + ' Captcha Solved')

            answer_field = self.browser.find_element_by_id('CaptchaInputText')
            answer_field.send_keys(result)
            answer_field.send_keys(Keys.ENTER)

            link = self.browser.find_element_by_id("link").text

            connected = False
            while not connected:
                try:
                    # Download the file
                    request.urlretrieve(link, start_date + '.zip', self.reporthook)

                    # Sometimes it downloads a empty file
                    file_size = os.stat(start_date + '.zip').st_size
                    if file_size <= 800000000:
                        os.remove(start_date + '.zip')
                        raise Exception('Downloading again')

                    connected = True
                except:
                    time.sleep(30)
                    continue

            upload_q.put((folder, start_date + '.zip'))
            time.sleep(10)


uploader1 = STACKUploader(4)
bot1 = NDWWebBot(1)
bot2 = NDWWebBot(2)
bot3 = NDWWebBot(3)

uploader1.start()
bot1.start()
bot2.start()
bot3.start()

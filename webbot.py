from selenium import webdriver
from urllib import request, error
import time, os
from datetime import datetime, timedelta
import requests
from twocaptchaapi import TwoCaptchaApi
import time
from PIL import Image
from threading import Thread
from queue import Queue
import easywebdav as wd

download_q = Queue()
upload_q = Queue()

start_date = "10-1-2017"
end_date = "29-11-2017"

start = datetime.strptime(start_date, "%d-%m-%Y")
stop = datetime.strptime(end_date, "%d-%m-%Y")

while start < stop:
    download_q.put(('Intensiteiten en snelheden', start.strftime("%d-%m-%Y"), start.strftime("%d-%m-%Y")))
    start = start + timedelta(days=1)


class STACKUploader(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.stack = None

    def __connect(self):
        self.stack = wd.connect(host="plengkeek.stackstorage.com", protocol="https", verify_ssl=True,
                                username='projectgroup', password='wearethebest')

    def upload(self, folder, file):
        self.stack.upload(file, "/remote.php/webdav/" + folder + '/' + file)

    def run(self):
        while True:
            self.__connect()
            if not upload_q.empty():
                file = upload_q.get()
                print('Uploading...')
                self.upload("historicaldata", file)
                os.remove(file)
            time.sleep(30)


class NDWWebBot(Thread):
    def __init__(self, id):
        Thread.__init__(self)
        self.id = id
        self.browser = webdriver.Chrome("/home/pleng/Desktop/chromedriver")

    def __open_browser(self):
        self.browser.get(('http://83.247.110.3/OpenDataHistorie'))
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        self.datatype = self.browser.find_element_by_id("productId")
        self.start_date = self.browser.find_element_by_id("fromDate")
        self.end_date = self.browser.find_element_by_id("untilDate")
        self.end_time  =self.browser.find_element_by_id("untilTime")
        self.next_button = self.browser.find_element_by_id("btnSubmit")


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
        with open('captcha' + str(self.id) + '.png', 'rb') as captcha_file:
            captcha = api.solve(captcha_file)

        answer = None
        while answer is None:
            try:
                answer = captcha.await_result()
            except:
                continue

        if len(answer) > 2:
            answer = self.__solve_captcha()
        return answer

    def run(self):
        while True:
            self.__open_browser()
            data_type, start_date, end_data = download_q.get()
            print(data_type, start_date, end_data)
            self.__fill_form(data_type, start_date, end_data)
            print('Solving Captcha...')

            result = self.__solve_captcha()
            answer_field = self.browser.find_element_by_id('CaptchaInputText')
            answer_field.send_keys(result)

            self.next_button.click()
            link = self.browser.find_element_by_id("link").text

            connected = False
            while not connected:
                try:
                    # Download the file
                    print(link)
                    request.urlretrieve(link, start_date + '.zip')

                    # Sometimes it downloads a empty file
                    file_size = os.stat(start_date + '.zip').st_size
                    if file_size <= 100000000:
                        os.remove(start_date + '.zip')
                        raise Exception('File to small')

                    connected = True
                except:
                    print('Link still unavailable')
                    time.sleep(30)
                    continue

            upload_q.put((start_date + '.zip'))
            time.sleep(10)

uploader = STACKUploader()
bot1 = NDWWebBot(1)

uploader.start()
bot1.start()
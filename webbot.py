from selenium import webdriver
from urllib import request, error
import time
import requests
from twocaptchaapi import TwoCaptchaApi
import time
from PIL import Image
from threading import Thread
from queue import Queue

q = Queue()

class NDWWebBot(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.browser = webdriver.Chrome()

    def __open_browser(self):
        self.browser.get(('http://83.247.110.3/OpenDataHistorie'))

        self.datatype = self.browser.find_element_by_id("productId")
        self.start_date = self.browser.find_element_by_id("fromDate")
        self.end_date = self.browser.find_element_by_id("untilDate")
        self.next_button = self.browser.find_element_by_id("btnSubmit")

    def __fill_form(self, data_type, start_date, end_data):
        self.datatype.send_keys(data_type)
        self.start_date.send_keys(start_date)
        self.end_date.send_keys(end_data)

    def __solve_captcha(self):
        # Save the captcha to file
        img = self.browser.find_element_by_id('CaptchaImage')
        location = img.location
        size = img.size
        self.browser.save_screenshot('screenshot.png')
        im = Image.open('screenshot.png')

        src = img.get_attribute('src')
        request.urlretrieve(src, 'captcha.png')

        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']

        im = im.crop((left, top, right, bottom))
        im.save('captcha.png')

        api = TwoCaptchaApi('b51dc904b4f0afc3693977440d8e2e02')
        with open('captcha.png', 'rb') as captcha_file:
            captcha = api.solve(captcha_file)
        answer = captcha.await_result()
        return answer

    def run(self):
        while True:
            self.__open_browser()
            data_type, start_date, end_data = q.get()
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
                    request.urlretrieve(link, 'file.zip')
                except error.URLError:
                    print('Link still unavailable')
                    time.sleep(30)
                    continue

                connected = True
                print('Downloading...')

thread1 = NDWWebBot()
q.put(('Configuratie', "1-11-2017", "7-11-2017"))
q.put(('Configuratie', "1-11-2017", "7-11-2017"))
thread1.start()

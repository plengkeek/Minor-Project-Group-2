import os
import time
import urllib.request
import zipfile
from collections import deque
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
import shutil
from PIL import Image
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from twocaptchaapi import TwoCaptchaApi

from stack import STACK
from logger import Logger


class NDWWebBot(Thread):
    def __init__(self, id, log_q):
        Thread.__init__(self)
        self.id = id
        self.log_q = log_q
        self.download_q_override = None
        self.attempts = 0

        self.url = 'http://83.247.110.3/OpenDataHistorie'
        self.open_browser()

        self.api = TwoCaptchaApi('b51dc904b4f0afc3693977440d8e2e02')

    def download(self, link, file_name, chunk_size):
        try:
            if shutil.disk_usage('./')[2]/1000000000 > 1.4:
                self.__log('Started downloading ' + file_name)
                with urllib.request.urlopen(link, timeout=10) as response, open(file_name, 'wb') as out_file:
                    while True:
                        buf = response.read(chunk_size * 1024)
                        if not buf:
                            break
                        out_file.write(buf)
                        out_file.flush()
                        self.__log('Downloaded ' + str(int(os.stat(file_name).st_size / (1024*1024))) + ' MB')
            else:
                self.__log('Not enough space, waiting for 2 minutes')
                time.sleep(120)
                raise Exception('Not an exception')

        except Exception as e:
            self.__log(str(e))
            time.sleep(5)
            self.download(link, file_name, chunk_size)

    def __log(self, message):
        self.log_q.append((self.id, datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'), message))

    def open_browser(self):
        self.__log('Opening browser')
        self.browser = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")
        self.__log('Loading webpage')
        self.browser.get(self.url)
        self.__log('Scrolling down')
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def at_webform(self):
        try:
            self.browser.find_element_by_id('productId')
            return True
        except NoSuchElementException:
            return False

    def load_webform(self):
        self.__log('Inspecting webform')
        self.datatype = self.browser.find_element_by_id('productId')
        self.start_day = self.browser.find_element_by_id('fromDate')
        self.end_day = self.browser.find_element_by_id('untilDate')
        self.start_hour = self.browser.find_element_by_id('fromTime')
        self.end_hour = self.browser.find_element_by_id('untilTime')
        self.captcha_answer = self.browser.find_element_by_id('CaptchaInputText')

    def fill_form(self, data_type, start_date, end_data):
        self.__log('Filling in the webform')
        self.datatype.send_keys(data_type)
        self.start_day.send_keys(start_date)
        self.end_day.send_keys(end_data)
        self.start_hour.click()
        self.end_hour.click()

    def submit_captcha(self):
        self.captcha_answer.send_keys(Keys.ENTER)

    def reload(self):
        self.__log('Reloading webpage')
        self.browser.get(self.url)
        self.__log('Scrolling down')
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def get_captcha(self):
        self.__log('Saving screenshot')
        img = self.browser.find_element_by_id('CaptchaImage')
        location, size = img.location_once_scrolled_into_view, img.size
        self.browser.save_screenshot('screenshot' + str(self.id) + '.png')

        self.__log('Locating captcha in screenshot')
        left = location['x']
        top = location['y']
        right = location['x'] + size['width']
        bottom = location['y'] + size['height']

        self.__log('Cropping and saving captcha image')
        im = Image.open('screenshot' + str(self.id) + '.png')
        im = im.crop((left, top, right, bottom))
        im.save('captcha' + str(self.id) + '.png')

    def solve_captcha(self, file_path):
        try:
            self.__log('Solving Captcha')
            with open(file_path, 'rb') as captcha_file:
                captcha = self.api.solve(captcha_file)
            answer = captcha.await_result()
            if len(answer) >= 2:
                return answer
            else:
                raise Exception(answer)
        except Exception as e:
            self.__log(str(e))
            time.sleep(5)
            return self.solve_captcha(file_path)

    def get_link(self):
        try:
            if not self.at_webform():
                link = self.browser.find_element_by_id("link").text
                # Gives exception if link is not clickable
                self.browser.find_element_by_link_text(link)
                return link
            else:
                self.__log('At the wrong page')
                time.sleep(30)
                return self.get_link()

        except Exception as e:
            self.__log('Link still inactive')
            time.sleep(60)
            return self.get_link()

    def check_file(self, file_path):
        try:
            zip = zipfile.ZipFile(file_path)
            if len(zip.filelist) >= (1441 - 1441*0.9):
                return 'Good'
            else:
                return 'Bad'
        except Exception as e:
            return 'Bad'

    def run(self):
        while not download_q.empty():
            try:

                self.reload()
                self.load_webform()

                if self.download_q_override is None:
                    folder, data_type, start_date, end_date = download_q.get()
                else:
                    folder, data_type, start_date, end_date = self.download_q_override
                self.__log('Trying ' + data_type + ' ' + start_date)

                self.__log('Looking for webform')
                if self.at_webform():
                    self.fill_form(data_type, start_date, end_date)
                    self.get_captcha()
                    solution = self.solve_captcha('captcha' + str(self.id) + '.png')
                    self.captcha_answer.send_keys(solution)
                    self.captcha_answer.send_keys(Keys.ENTER)

                    time.sleep(5)
                    if self.at_webform():
                        self.download_q_override = folder, data_type, start_date, end_date
                        raise Exception('Wrong Captcha answer')

                else:
                    self.__log('Not at webform')
                    self.download_q_override = folder, data_type, start_date, end_date
                    raise Exception('Wrong Captcha answer')

                link = self.get_link()
                if data_type == 'Reistijden':
                    file_name = 'r' + start_date + '.zip'
                else:
                    file_name = start_date + '.zip'

                self.download(link, file_name=file_name, chunk_size=1024*25)
                if self.check_file(file_name) != 'Good':
                    self.download_q_override = folder, data_type, start_date, end_date
                    raise Exception('Corrupted or incomplete zip, FUCK YOU NDW!')
                else:
                    self.download_q_override = None

                upload_q.put((folder, file_name))
                time.sleep(30)

            except Exception as e:
                self.__log(str(e))
                time.sleep(30)


stack_parameters = {'host': 'plengkeek.stackstorage.com',
                    'username': 'projectgroup',
                    'password': 'wearethebest'}

download_q = Queue()
upload_q = Queue()
log_q = deque(maxlen=3*10)

stack = STACK(0, upload_q, log_q, stack_parameters)
stack.start()

start = datetime.strptime("01-01-2017", "%d-%m-%Y")
stop = datetime.strptime("20-12-2017", "%d-%m-%Y")

traveltime_dates = stack.ls('/remote.php/webdav/historicaldata/traveltime')
traveltime_dates = [file[0][-14:-4] for file in traveltime_dates]

intensityandspeed_dates = stack.ls('/remote.php/webdav/historicaldata/intensityandspeed')
intensityandspeed_dates = [file[0][-14:-4] for file in intensityandspeed_dates]

while start < stop:
    if start.strftime("%d-%m-%Y") not in traveltime_dates:
        download_q.put(
            ('historicaldata/traveltime', 'Reistijden', start.strftime("%d-%m-%Y"), start.strftime("%d-%m-%Y")))
    if start.strftime("%d-%m-%Y") not in intensityandspeed_dates:
        download_q.put(('historicaldata/intensityandspeed', 'Intensiteiten en snelheden', start.strftime("%d-%m-%Y"),
            start.strftime("%d-%m-%Y")))
    start = start + timedelta(days=1)

logger = Logger(log_q)
logger.start()

bot1 = NDWWebBot(1, log_q)
bot2 = NDWWebBot(2, log_q)
bot3 = NDWWebBot(3, log_q)

bot1.start()
bot2.start()
bot3.start()
from selenium import webdriver
from urllib import request, error
import time
import requests
from twocaptchaapi import TwoCaptchaApi
import time
from PIL import Image


'''''
You need chrome te be installed on your computer!
'''''

browser = webdriver.Chrome()
browser.get(('http://83.247.110.3/OpenDataHistorie'))

datatype = browser.find_element_by_id("productId")
start_date = browser.find_element_by_id("fromDate")
end_date = browser.find_element_by_id("untilDate")
next_button = browser.find_element_by_id("btnSubmit")

#datatype.send_keys("Intensiteiten en snelheden")
datatype.send_keys("Configuratie")
start_date.send_keys("1-11-2017")
end_date.send_keys("7-11-2017")

# Save the captcha to file
img = browser.find_element_by_id('CaptchaImage')
location = img.location
size = img.size
browser.save_screenshot('screenshot.png')
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
answer_field = browser.find_element_by_id('CaptchaInputText')
answer_field.send_keys(answer)

next_button.click()
link = browser.find_element_by_id("link").text

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

print('Downloading the file now! :D')






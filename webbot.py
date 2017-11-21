from selenium import webdriver
from urllib import request, error
import time

'''''
You need chrome te be installed on your computer!
'''''

browser = webdriver.Chrome()
browser.get(('http://83.247.110.3/OpenDataHistorie'))

datatype = browser.find_element_by_id("productId")
start_date = browser.find_element_by_id("fromDate")
end_date = browser.find_element_by_id("untilDate")
next_button = browser.find_element_by_id("btnSubmit")

datatype.send_keys("Intensiteiten en snelheden")
start_date.send_keys("1-11-2017")
end_date.send_keys("7-11-2017")


input('Press enter after captcha')

next_button.click()
link = browser.find_element_by_id("link").text

connected = False
while not connected:
    try:
        # Download the file
        request.urlretrieve(link)
    except error.URLError:
        print('Link still unavailable')
        time.sleep(30)
        continue

    connected = True

print('Downloading the file now! :D')






from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementNotInteractableException, NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import shutil
import time
import os
import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import csv

chrome_options = webdriver.ChromeOptions()
base_path = f"{os.getcwd()}\\downloadfolder" # local, gets current working directory
base_path
prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
chrome_options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.maximize_window()

url = 'https://www.gulftalent.com'
middleEast_countries = ['uae','saudi-arabia','qatar','kuwait','egypt','jordan','oman','bahrain','iraq','lebanon','libya']


for c in middleEast_countries:
    url_middleEast = f'https://www.gulftalent.com/{c}/jobs'
    #print(url_middleEast)
    driver.get(url_middleEast)
    page_source = driver.page_source               
    soup = BeautifulSoup(page_source, 'html.parser')
    
    table = soup.find('table')
    rows = table.find_all('tr')
    with open(f'jobs{c}.csv','w',encoding='utf8',newline = '') as f:
        thewriter = csv.writer(f)
        header = ['position','location','date','img','links','types','descriptions']
        thewriter.writerow(header)
        for row in rows[1:]:
            a = row.find('a')['href']
            l = f'{url}{a}'
            link = [f'{url}{a}']
            driver.get(l)
            time.sleep(5)
            page_source = driver.page_source                   
            soup = BeautifulSoup(page_source, 'html.parser')
            Emp = soup.find(class_ = 'space-bottom-sm')
            Employment = [Emp.find('span').get_text(strip=True)]
            #Employment = [Employment]
            cont = soup.find(class_= 'panel-body content-visibility-auto')
            data = cont.find_all('p')
            job_description = data[0]
            job_description=[job_description.get_text(strip=True)]

            cells = row.find_all(['td','th'])
            cells_text = [cell.get_text(strip=True) for cell in cells] + link + Employment + job_description #+ requirements #+ description
            #print(cells_text)
            thewriter.writerow(cells_text)
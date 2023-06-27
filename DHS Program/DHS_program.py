import requests
from bs4 import BeautifulSoup
import wget
import logging
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
import os
import urllib.request
from urllib import request
import pdfplumber
import tabula 
import csv
from Hashing.HashScrapedData import _hashing
import pandas as pd
from unidecode import unidecode
import pandas as pd
base_path='\\\\10.30.31.77\\data_collection_dump\\RawData\\DHS_program'

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
import json
import urllib.request
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():

    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()

    result = requests.get('https://api.dhsprogram.com/#/index.html')
    soup = BeautifulSoup(result.content,'html.parser')
    url = 'https://api.dhsprogram.com/'
    side = soup.find(class_='SideNavDiv')
    list_side = side.find(class_='navList')
    list_apiRef = list_side.find('ul')
    aspect = list_apiRef.find_all('a')[2].text
    indic_data = list_apiRef.find_all('a')[2]['href']
    link_indic_data = url + indic_data

    driver.get(link_indic_data)
    time.sleep(2)

    api_urls = driver.find_elements(By.CLASS_NAME,'apiReferenceURL')
    data_DHS = api_urls[10].text
    req = urllib.request.urlopen(data_DHS)
    resp = json.loads(req.read())
    my_data = resp['Data']
    df_data = pd.DataFrame(my_data)
    u = df_data.select_dtypes(object)
    df_data[u.columns] = u.apply(
        lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))
    df_data[df_data.columns] = df_data[df_data.columns].replace({'-':'','_':''}, regex=True)
    df_data.to_csv(f'{base_path}\\{aspect}.csv',index=False, encoding="utf-8")

    contents = driver.find_elements(By.CLASS_NAME,"apiText")
    concept = contents[0].text
    fields = contents[1].find_element(By.TAG_NAME,'a').get_attribute('href')
    res_fields = requests.get(fields)            
    soup_fields = BeautifulSoup(res_fields.content, 'html.parser')
    rows = soup_fields.find_all('tr')
    l = []
    for tr in rows[3:]:
        cells = tr.find_all(['td','th'])
        cells_text = [cell.get_text(strip=True) for cell in cells]
        l.append(cells_text)
    df = pd.DataFrame(l).iloc[:,1:]
    desc = []
    for i in range(len(df.index)):
        f = df.iloc[i].to_list()
        f = [' : '.join(map(str,f))]
        desc.append(f[0])
    descr = [' \n\n '.join(map(str,desc))][0]
    descr = descr.replace('\n\t\t\t\t\t\t\t\t',' ')

    try:
        BodyDict = {
        "JobPath":f'//10.30.31.77/data_collection_dump/RawData/DHS_program/{aspect}.csv', #* Point to downloaded data for conversion 
        "JsonDetails":{
                ## Required
                "organisation": "third-parties",
                "source": "DHS-Program",
                "source_description" : "The Demographic and Health Surveys (DHS) Program has collected, analyzed, and disseminated accurate and representative data on population, health, HIV, and nutrition through more than 300 surveys in over 90 countries.",
                "source_url" : "https://api.dhsprogram.com/#/index.html",
                "table" : aspect,
                "description" : descr, 
                ## Optional
                "JobType": "JSON",
                "CleanPush": True,
                "Server": "str",
                "UseJsonFormatForSQL":  False,
                "CleanReplace":True,
                "MergeSchema": False,
                "tags": [{
                    "name": aspect
                }],
                "additional_data_sources": [{
                    "name": "",
                    "url" : ""
                }],
                "limitations":"",
                "concept": concept,
                "periodicity":  "",
                "topic":  aspect,
                "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified": "",                #* ""               ""                  ""              ""
                "TriggerTalend" :  False,    #* initialise to True for production
                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData" #* initialise as empty string for production.
            }
        }
        tablenom = BodyDict['JsonDetails']['table']

        hashmessage = _hashing(BodyDict['JsonDetails']['source'], tablenom, BodyDict["JobPath"])

        if hashmessage["Trigger_InferSchema"] == True and hashmessage["Success"] == True:

            TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
            TriggerInferShemaToJsonAPIClass.TriggerAPI()
            logging.info(f"Conversion successful - {tablenom}, hashmessage: {hashmessage['message']}")

        # logging.info(f"Conversion successful for {dataset}")
        elif hashmessage['Success'] == True and hashmessage['Trigger_InferSchema'] == False:

            # dont trigger conversion nor talend
            logging.info(f"{hashmessage['message']}")

        elif hashmessage['Success'] == False:
            logging.info(f"Hashing error or Unexpected Issue: {hashmessage['message']}")
    except  Exception as err:
        print(err)


    aspects = []
    links = []
    result = requests.get('https://api.dhsprogram.com/#/index.html')
    soup = BeautifulSoup(result.content,'html.parser')
    url = 'https://api.dhsprogram.com/'
    side = soup.find(class_='SideNavDiv')
    list_side = side.find(class_='navList')
    list_apiRef = list_side.find('ul')
    elems_apiref = list_apiRef.find_all('a')#[2]['href']
    for elem in elems_apiref:
        aspect = elem.text
        aspects.append(aspect)
        link = url + elem['href']
        links.append(link)
    links = [links[3]] + links[5:7] + links[8:10]
    aspects = [aspects[3]] + aspects[5:7] + aspects[8:10]

    for i in range(len(links)):
        # link = links[i]
        driver.get(links[i])
        time.sleep(3)
        aspect = aspects[i]
        # cookies = driver.find_element(By.CLASS_NAME,'cc-compliance').find_element(By.TAG_NAME,'a')
        # cookies.click()
        time.sleep(5)
        concept = driver.find_element(By.CLASS_NAME,'apiText').text
        api_urls = driver.find_elements(By.CLASS_NAME,'apiReferenceURL')[0].text
        print(api_urls)
        req = urllib.request.urlopen(api_urls)
        time.sleep(1)
        resp = json.loads(req.read())
        time.sleep(5)
        my_data = resp['Data']
        df_data = pd.DataFrame(my_data)
        u = df_data.select_dtypes(object)
        df_data[u.columns] = u.apply(
            lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))
        df_data[df_data.columns] = df_data[df_data.columns].replace({'-':'','_':''}, regex=True)
        df_data.to_csv(f'{base_path}\\{aspect}.csv',index=False, encoding="utf-8")
        contents = driver.find_elements(By.CLASS_NAME,"apiText")
        fields = contents[1].find_element(By.TAG_NAME,'a').get_attribute('href')
        res_fields = requests.get(fields)            
        soup_fields = BeautifulSoup(res_fields.content, 'html.parser')
        rows = soup_fields.find_all('tr')
        l = []
        for tr in rows[3:]:
            cells = tr.find_all(['td','th'])
            cells_text = [cell.get_text(strip=True) for cell in cells]
            l.append(cells_text)
        df = pd.DataFrame(l).iloc[:,1:]
        desc = []
        for i in range(len(df.index)):
            f = df.iloc[i].to_list()
            f = [' : '.join(map(str,f))]
            desc.append(f[0])                                                   
        descr = [' \n\n '.join(map(str,desc))][0]
        descr = descr.replace('\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t',' ').replace('\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t',' ').replace('\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t',' ')
        try:
            BodyDict = {
            "JobPath":f'//10.30.31.77/data_collection_dump/RawData/DHS_program/{aspect}.csv', #* Point to downloaded data for conversion 
            "JsonDetails":{
                    ## Required
                    "organisation": "third-parties",
                    "source": "DHS-Program",
                    "source_description" : "The Demographic and Health Surveys (DHS) Program has collected, analyzed, and disseminated accurate and representative data on population, health, HIV, and nutrition through more than 300 surveys in over 90 countries.",
                    "source_url" : "https://api.dhsprogram.com/#/index.html",
                    "table" : aspect,
                    "description" : descr, 
                    ## Optional
                    "JobType": "JSON",
                    "CleanPush": True,
                    "Server": "str",
                    "UseJsonFormatForSQL":  False,
                    "CleanReplace":True,
                    "MergeSchema": False,
                    "tags": [{
                        "name": aspect
                    }],
                    "additional_data_sources": [{
                        "name": "",
                        "url" : ""
                    }],
                    "limitations":"",
                    "concept":  concept,
                    "periodicity":  "",
                    "topic":  aspect,
                    "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": "",                #* ""               ""                  ""              ""
                    "TriggerTalend" :  False,    #* initialise to True for production
                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData" #* initialise as empty string for production.
                }
            }
            tablenom = BodyDict['JsonDetails']['table']

            hashmessage = _hashing(BodyDict['JsonDetails']['source'], tablenom, BodyDict["JobPath"])

            if hashmessage["Trigger_InferSchema"] == True and hashmessage["Success"] == True:

                TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
                TriggerInferShemaToJsonAPIClass.TriggerAPI()
                logging.info(f"Conversion successful - {tablenom}, hashmessage: {hashmessage['message']}")

            # logging.info(f"Conversion successful for {dataset}")
            elif hashmessage['Success'] == True and hashmessage['Trigger_InferSchema'] == False:

                # dont trigger conversion nor talend
                logging.info(f"{hashmessage['message']}")

            elif hashmessage['Success'] == False:
                logging.info(f"Hashing error or Unexpected Issue: {hashmessage['message']}")
        except  Exception as err:
            print(err)

execute() 


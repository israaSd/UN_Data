from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UN-DRR"
import urllib.request
import logging
import pandas as pd
from re import search
import datetime
from Hashing.HashScrapedData import _hashing

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
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs\\UN-DRR\\drr_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = f"\\\\10.30.31.77\\data_collection_dump\\RawData\\UN-DRR" # local, gets current working directory
    base_path
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://data-undrr.opendata.arcgis.com/search?collection=Dataset')
    time.sleep(5)
    parent_node=driver.find_element(By.XPATH,'/html/body/div[7]/div[2]/div/div[1]/div[3]/div/div/div/div[2]/div[2]')
    child_node = parent_node.find_elements(By.TAG_NAME,'ul')
    for ul in child_node:
        all_li = ul.find_elements(By.TAG_NAME,'li')
        BodyDict = {
            "JobPath":"", #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "un-agencies",
                    "source": "UNDRR",
                    "source_description" : "This is the platform for exploring and downloading GIS data, discovering and building apps, and engaging others to solve important issues around disaster risk reduction. You can analyze and combine datasets using maps, as well as develop new web and mobile applications. Let's achieve our goals together.",
                    "source_url" : "https://data-undrr.opendata.arcgis.com/search?collection=Dataset",
                    "table" : "",
                    "description" : "", 
                    ## Optional
                    "JobType": "JSON",
                    "CleanPush": True,
                    "Server": "str",
                    "UseJsonFormatForSQL":  False,
                    "CleanReplace":True,
                    "MergeSchema": False,
                    "tags": [{
                        "name": ""
                    }],
                    "additional_data_sources": [{
                        "name": ""
                    }],
                    "limitations":"",
                    "concept":  "",
                    "periodicity":  "",
                    "topic":  "",
                    "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": "",                #* ""               ""                  ""              ""
                    "TriggerTalend" :  False,    #* initialise to True for production
                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/UN-DRR" #* initialise as empty string for production.
                }
            }
        try:
            for li in all_li:
                titles = li.find_elements(By.TAG_NAME,'h3')
                for title in titles:
                    if title.text=='Sheet1':
                        ul_2 = parent_node.find_element(By.CLASS_NAME,'metadata-list')
                        all_li_2 = ul_2.find_elements(By.TAG_NAME,'li')
                        concept = all_li_2[0].text
                        concept =  concept.split(': ')[1]
                        last_modified = all_li_2[1].text
                        last_modified = last_modified.split(': ')[1].replace(',','')
                        last_modified = datetime.datetime.strptime(last_modified, '%B %d %Y').strftime('%Y-%m-%d')
                        ul_3 = parent_node.find_element(By.CLASS_NAME,'metadata-list.metadata-list-2')
                        all_li_3 = ul_3.find_elements(By.TAG_NAME,'li')
                        tag = all_li_3[1].text
                        tag = tag.split(': ')[1]
                        topic = parent_node.find_element(By.CLASS_NAME,'owner-source').text
                        link=title.find_element(By.TAG_NAME,'a').get_attribute('href')
                        driver.get(link)
                        time.sleep(22)
                        ul_publiched = driver.find_element(By.CLASS_NAME,'content-metadata-list')
                        all_li = ul_publiched.find_elements(By.TAG_NAME,'li')
                        publiched = all_li[3].find_element(By.TAG_NAME,'div').text
                        publiched = publiched.replace(',','')
                        created = datetime.datetime.strptime(publiched, '%B %d %Y').strftime('%Y-%m-%d')
                        download=driver.find_element(By.CLASS_NAME,"side-panel-ref").find_element(By.CLASS_NAME,'btn.btn-default.btn-block')
                        download.click()
                        time.sleep(14)
                        download_csv = driver.find_element(By.CLASS_NAME,'dataset-download-card')
                        shadow = Shadow(driver)
                        download_c = shadow.find_element("calcite-button")
                        download_c.click()
                        time.sleep(3)
                        # filename = max([f for f in os.listdir(base_path)])
                        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                        filename = os.path.split(file)[1]
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/UN-DRR/{filename}'
                            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
                            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] =  topic
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["last_modified"] =  last_modified
                            BodyDict["JsonDetails"]["created"] =  created
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
                        driver.back()
                        time.sleep(7)
                    elif title.text=='CompleteData 2017':
                        ul_2 = parent_node.find_element(By.CLASS_NAME,'metadata-list')
                        all_li_2 = ul_2.find_elements(By.TAG_NAME,'li')
                        concept = all_li_2[0].text
                        concept =  concept.split(': ')[1]
                        last_modified = all_li_2[1].text
                        last_modified = last_modified.split(': ')[1].replace(',','')
                        last_modified = datetime.datetime.strptime(last_modified, '%B %d %Y').strftime('%Y-%m-%d')
                        ul_3 = parent_node.find_element(By.CLASS_NAME,'metadata-list.metadata-list-2')
                        all_li_3 = ul_3.find_elements(By.TAG_NAME,'li')
                        tag = all_li_3[1].text
                        topic = parent_node.find_element(By.CLASS_NAME,'owner-source').text
                        link=title.find_element(By.TAG_NAME,'a').get_attribute('href')
                        driver.get(link)
                        time.sleep(22)
                        ul_publiched = driver.find_element(By.CLASS_NAME,'content-metadata-list')
                        all_li = ul_publiched.find_elements(By.TAG_NAME,'li')
                        publiched = all_li[3].find_element(By.TAG_NAME,'div').text
                        publiched = publiched.replace(',','')
                        created = datetime.datetime.strptime(publiched, '%B %d %Y').strftime('%Y-%m-%d')
                        download=driver.find_element(By.CLASS_NAME,"side-panel-ref").find_element(By.CLASS_NAME,'btn.btn-default.btn-block')
                        download.click()
                        time.sleep(14)
                        download_csv = driver.find_element(By.CLASS_NAME,'dataset-download-card')
                        shadow = Shadow(driver)
                        download_c = shadow.find_element("calcite-button")
                        download_c.click()
                        time.sleep(3)
                        # filename = max([f for f in os.listdir(base_path)])
                        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                        filename = os.path.split(file)[1]
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/UN-DRR/{filename}'
                            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
                            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] =  topic
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["last_modified"] =  last_modified
                            BodyDict["JsonDetails"]["created"] =  created
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
                        driver.back()
                        time.sleep(7)
        except  Exception as err:
            print(err)

execute()
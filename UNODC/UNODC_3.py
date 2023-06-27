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
from Hashing.HashScrapedData import _hashing
import shutil
import time
import os
import logging
import datetime
import pandas as pd
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_3'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNODC_3_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_3' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()
    url = 'https://dataunodc.un.org/'
    driver.get(url)
    content = driver.find_element(By.ID,'wrapper')
    theme = content.find_elements(By.CLASS_NAME,'theme')[11]
    tag = theme.text
    click = theme.find_element(By.TAG_NAME,'a')
    theme.click()
    time.sleep(2)
    concept = driver.find_element(By.CLASS_NAME,'field-item.even').find_element(By.TAG_NAME,'p').text
    topics = driver.find_elements(By.CLASS_NAME,'rtecenter')
    topics = [topics[0]] + topics[2:]
    for topic in topics:
        title = topic.text
        link = topic.find_element(By.TAG_NAME,'a').get_attribute('href')
        driver.get(link)
        time.sleep(2)
        note = driver.find_elements(By.CLASS_NAME,'field-items')[2].text.replace('\n','')
        import urllib.request
        download = driver.find_element(By.CLASS_NAME,'panel-panel-inner').find_element(By.TAG_NAME,'a').get_attribute('href')
        urllib.request.urlretrieve(download,f"{base_path}\\COVID-19 {title}.xlsx")
        def latest_download_file():
            path = base_path
            os.chdir(path)
            files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
            newest = files[-1]

            return newest

        fileends = "crdownload"
        while "crdownload" == fileends:
            time.sleep(3) 
            newest_file = latest_download_file()
            if "crdownload" in newest_file:
                fileends = "crdownload"
                # time.sleep(5)
            else:
                fileends = "None"
        latest_download_file() 
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        try:
            BodyDict = {
            "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNODC_3/COVID-19 {title}.xlsx", #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "un-agencies",
                    "source": "UNODC",
                    "source_description" : "The UNODC thematic programme on research, trend analysis and forensics undertakes  thematic research programmes, manages global and regional data collections, provides scientific and forensic services, defines research standards, and supports Member States to strengthen their data collection, research and forensics capacity.",
                    "source_url" : "https://dataunodc.un.org/",
                    "table" : f'COVID-19 {title}',
                    "description" : note,
                    ## Optional
                    "JobType": "JSON",
                    "CleanPush": True,
                    "Server": "str",
                    "UseJsonFormatForSQL":  False,
                    "CleanReplace":True,
                    "MergeSchema": False,
                    "tags": [{
                        "name": tag
                    }],
                    "additional_data_sources": [{
                        "name": '',
                        "url" : ""
                    }],
                    "limitations":"",
                    "concept":  concept,
                    "periodicity":  "",
                    "topic": f'COVID-19 {title}',
                    "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": '',                #* ""               ""                  ""              ""
                    "TriggerTalend" : False,    #* initialise to True for production
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
        driver.back()
        time.sleep(2)

execute()
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
import urllib.request
from Hashing.HashScrapedData import _hashing
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
import pandas as pd
import numpy as np
import logging
import datetime
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_2'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNODC_2_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_2' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    url = 'https://dataunodc.un.org/'
    driver.get(url)

    content = driver.find_element(By.ID,'wrapper')
    themes = content.find_elements(By.CLASS_NAME,'theme')[2:11]
    for theme in themes:
        tag = theme.text
        click = theme.find_element(By.TAG_NAME,'a')
        theme.click()
        time.sleep(2)
        import urllib.request
        try:
            title = driver.find_element(By.CLASS_NAME,'title').text
        except:
            title = driver.find_element(By.CLASS_NAME,'last.active').text
        dataset = driver.find_element(By.CLASS_NAME,'rteright')
        link_data = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
        urllib.request.urlretrieve(link_data,f"{base_path}\\{title}.xlsx")
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
        import datetime
        if tag == 'SDGs':
            df = pd.read_excel(file)
            df.to_csv(fr'{base_path}\\{tag}.csv',index=False, encoding="utf-8")
            update = ''
        else:
            df = pd.read_excel(file)
            update = df.iloc[0,0]
            update = datetime.datetime.strptime(update, "%d/%m/%Y").strftime('%Y-%m-%d')
            df.columns = df.iloc[1,:]
            df = df[2:]
            df.to_csv(fr'{base_path}\\{tag}.csv',index=False, encoding="utf-8")
        try:
            link_metadata = dataset.find_elements(By.TAG_NAME,'a')[1].get_attribute('href')
            urllib.request.urlretrieve(link_metadata,f"{base_path}\\{title}.pdf")
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
            file_pdf = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
            import pdfplumber
            import datetime
            conten=[]
            with pdfplumber.open(file_pdf) as pdf:
                text = pdf.pages
                for te in text:
                    content = te.extract_text()
                    conten.append(content)
            content_all = ' '.join([str(elem) for elem in conten])
            concept = content_all.split('Metadata Information')[1].split('Dataset characteristics')[0].replace('\n','')
            last_update = content_all.split('Last update: ')[1].split('Base period: ')[0].replace(' \n','')
            last_update = datetime.datetime.strptime(last_update, "%d/%m/%Y").strftime('%Y-%m-%d')
            source = content_all.split('Data source(s): ')[1].split('Contact')[0].replace(' \n','')
            defin = content_all.split('Statistical concepts and definitions')[1].split('Data sources and method of collection')[0].replace(' \n','')
            try:
                BodyDict = {
                "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNODC_2/{tag}.csv", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNODC",
                        "source_description" : "The UNODC thematic programme on research, trend analysis and forensics undertakes  thematic research programmes, manages global and regional data collections, provides scientific and forensic services, defines research standards, and supports Member States to strengthen their data collection, research and forensics capacity.",
                        "source_url" : "https://dataunodc.un.org/",
                        "table" : tag,
                        "description" : defin,
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
                            "name": source,
                            "url" : ""
                        }],
                        "limitations":"",
                        "concept":  concept,
                        "periodicity":  "",
                        "topic":  tag,
                        "created": last_update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified": last_update,                #* ""               ""                  ""              ""
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
        except:
            try:
                BodyDict = {
                "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNODC_2/{tag}.csv", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNODC",
                        "source_description" : "The UNODC thematic programme on research, trend analysis and forensics undertakes  thematic research programmes, manages global and regional data collections, provides scientific and forensic services, defines research standards, and supports Member States to strengthen their data collection, research and forensics capacity.",
                        "source_url" : "https://dataunodc.un.org/",
                        "table" : tag,
                        "description" : tag,
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
                        "concept":  '',
                        "periodicity":  "",
                        "topic":  tag,
                        "created": update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified": update,                #* ""               ""                  ""              ""
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
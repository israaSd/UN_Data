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
from Hashing.HashScrapedData import _hashing
from bs4 import BeautifulSoup
import shutil
import time
import os
import textwrap
import pandas as pd
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
import logging
import datetime
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNOPS'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNOPS_out.log"),
                                    logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNOPS" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://data.unops.org/index.htm#SegmentCode=ORG&FocusCode=DATA_PROJECTS&EntityCode=ORG_CODE&EntityValue=UNOPS##SectionCode=OVERVIEW')
    time.sleep(2)
    driver.refresh()
    time.sleep(3)

    update = driver.find_element(By.ID,'tbtext-1062').text.split(': ')[1]
    update = datetime.datetime.strptime(update, "%d-%b-%Y").strftime('%Y-%m-%d')
    driver.find_element(By.ID,'button-1039').click()
    def latest_download_file():
        path = base_path
        os.chdir(path)
        files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
        newest = files[-1]

        return newest

    fileends = "crdownload"
    while "crdownload" == fileends:
        time.sleep(10) 
        newest_file = latest_download_file()
        if "crdownload" in newest_file:
            fileends = "crdownload"
            # time.sleep(5)
        else:
            fileends = "None"
    time.sleep(1)
    latest_download_file() 
    time.sleep(1)
    os.rename(f'{base_path}\\Download.csv',f'{base_path}\\Projects.csv')
    try:
        BodyDict = {
        "JobPath":f'//10.30.31.77/data_collection_dump/RawData/UNOPS/Projects.csv', #* Point to downloaded data for conversion 
        "JsonDetails":{
                ## Required
                "organisation": "un-agencies",
                "source": "UNOPS",
                "source_description" : "The United Nations Office for Project Services is a United Nations agency dedicated to implementing infrastructure and procurement projects for the United Nations System, international financial institutions, governments and other partners around the world.",
                "source_url" : "https://data.unops.org/index.htm#SegmentCode=ORG&FocusCode=DATA_PROJECTS&EntityCode=ORG_CODE&EntityValue=UNOPS##SectionCode=OVERVIEW",
                "table" : 'Projects',
                "description" : 'Total funded amount is the total funds provided by the funding source(s) for the implementation of the project to date. Total delivery is the total amount expended for the implementation of the project to date. All amounts are in US Dollars.', 
                ## Optional
                "JobType": "JSON",
                "CleanPush": True,
                "Server": "str",
                "UseJsonFormatForSQL":  False,
                "CleanReplace":True,
                "MergeSchema": False,
                "tags": [{
                    "name": 'Projects'
                }],
                "additional_data_sources": [{
                    "name": "",
                    "url" : ""
                }],
                "limitations":"",
                "concept":  '',
                "periodicity":  "",
                "topic":  'Projects',
                "created": update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified": update,                #* ""               ""                  ""              ""
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

    driver.get('https://data.unops.org/index.htm#SegmentCode=ORG&FocusCode=DATA_PROJECTS&EntityCode=ORG_CODE&EntityValue=UNOPS##SectionCode=OVERVIEW')
    time.sleep(2)
    driver.refresh()
    time.sleep(3)

    projects_id = []
    from selenium.common.exceptions import TimeoutException
    table = driver.find_element(By.TAG_NAME,'tbody')
    dataset = table.find_elements(By.CLASS_NAME,'x-grid-cell.x-grid-cell-gridcolumn-1078')
    for j in range(77):
        for x in dataset:
            data_dataset = x.find_element(By.TAG_NAME,'div').find_element(By.TAG_NAME,'a').get_attribute('onclick')
            data_dataset = data_dataset.split(", '")[-1].split("')")[0]
            if data_dataset in projects_id:
                pass
            else:
                projects_id.append(data_dataset)
            
        next_table = driver.find_element(By.ID,'button-1096')
        next_table.click()
        time.sleep(7)
        timeout = 15
        try:
            element_present = EC.presence_of_element_located((By.XPATH,'/html/body/table/tbody/tr/td/div/div/table/tbody/tr[6]/td/div/div[1]/div/table/tbody/tr[2]/td/div/div/div[1]/div[2]/div/div[1]/table/tbody/tr/td/div/div/table/tbody/tr[2]/td/div/div[2]/div/div/table/tbody/tr/td/div/div[2]/div/table/tbody/tr[2]/td[2]'))
            WebDriverWait(driver, timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load next table")
        table = driver.find_element(By.TAG_NAME,'tbody')
        dataset = table.find_elements(By.CLASS_NAME,'x-grid-cell.x-grid-cell-gridcolumn-1078')


    from selenium.common.exceptions import TimeoutException
    for i in projects_id:
        url = f'https://data.unops.org/index.htm#SegmentCode=ORG&FocusCode=DATA_PROJECTS&EntityCode=PROJECT_ID&EntityValue={i}##SectionCode=OVERVIEW'
        driver.get(url)
        time.sleep(2)
        driver.refresh()
        time.sleep(5)
        timeout = 15
        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME,'x-box-inner.x-horizontal-box-overflow-body'))
            WebDriverWait(driver, timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load dataset")
        driver.find_elements(By.CLASS_NAME,'x-box-inner.x-horizontal-box-overflow-body')[2].find_elements(By.CLASS_NAME,'x-tab.x-box-item.x-tab-default.x-noicon.x-tab-noicon.x-tab-default-noicon.x-top.x-tab-top.x-tab-default-top')[1].click()
        time.sleep(2)
        data = driver.find_elements(By.CLASS_NAME,'x-toolbar-text.x-box-item.x-toolbar-item.x-toolbar-text-default')[2].text
        if '0' in data :
            continue
        else:
            summary = []
            title = driver.find_element(By.TAG_NAME,'thead').text.replace(':','').replace('/',',').replace('"','').replace('[','-').replace(']','').replace('’','')
            title = f'{i} - {title}'
            title_file = textwrap.shorten(title, width=150).replace('[','').replace(']','')
            # print(title)
            table_summary = driver.find_element(By.CLASS_NAME,'summary')
            rows = table_summary.find_elements(By.TAG_NAME,'tr')
            for row in rows:
                summary.append(row.text)
            source = summary[0].replace('\n',' ') + ', ' + summary[1].replace('\n',' ')
            description = summary[2].replace('\n',' ') + ', ' + summary[3].replace('\n',' ') + ', ' + summary[5].replace('\n',' ') + ', ' + summary[6].replace('\n',' ')
            created = summary[4].split(': ')[1].split(' to')[0].replace(' ','')
            created = datetime.datetime.strptime(created, "%d-%b-%Y").strftime('%Y-%m-%d')
            last_update = summary[4].split(' to ')[1].replace(' ','')
            last_update = datetime.datetime.strptime(last_update, "%d-%b-%Y").strftime('%Y-%m-%d')
            # print(source,description,created,last_update)
            driver.find_element(By.ID,'button-1039').click()
            def latest_download_file():
                path = base_path
                os.chdir(path)
                files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                newest = files[-1]

                return newest

            fileends = "crdownload"
            while "crdownload" == fileends:
                time.sleep(12) 
                newest_file = latest_download_file()
                if "crdownload" in newest_file:
                    fileends = "crdownload"
                    # time.sleep(5)
                else:
                    fileends = "None"
            time.sleep(1)
            latest_download_file() 
            time.sleep(1)
            os.rename(f'{base_path}\\Download.csv',f'{base_path}\\{title_file}.csv')
            time.sleep(1)
            try:
                BodyDict = {
                "JobPath":f'//10.30.31.77/data_collection_dump/RawData/UNOPS/{title_file}.csv', #* Point to downloaded data for conversion 
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNOPS",
                        "source_description" : "The United Nations Office for Project Services is a United Nations agency dedicated to implementing infrastructure and procurement projects for the United Nations System, international financial institutions, governments and other partners around the world.",
                        "source_url" : "https://data.unops.org/index.htm#SegmentCode=ORG&FocusCode=DATA_PROJECTS&EntityCode=ORG_CODE&EntityValue=UNOPS##SectionCode=OVERVIEW",
                        "table" : title_file,
                        "description" : description, 
                        ## Optional
                        "JobType": "JSON",
                        "CleanPush": True,
                        "Server": "str",
                        "UseJsonFormatForSQL":  False,
                        "CleanReplace":True,
                        "MergeSchema": False,
                        "tags": [{
                            "name": 'Projects'
                        }],
                        "additional_data_sources": [{
                            "name": source,
                            "url" : ""
                        }],
                        "limitations":"",
                        "concept":  '',
                        "periodicity":  "",
                        "topic":  title,
                        "created": created,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified": last_update,                #* ""               ""                  ""              ""
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


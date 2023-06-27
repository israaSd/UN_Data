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
import urllib.request
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
import pandas as pd
import numpy as np
import logging
import datetime
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\OHCHR'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/OHCHR_out.log"),
                                logging.StreamHandler()], level=logging.INFO)

    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\OHCHR' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    url = 'https://indicators.ohchr.org/'
    driver.get(url)
    time.sleep(2)


    top = driver.find_element(By.ID,'rectfctn_dd')
    select_element = top.find_element(By.CLASS_NAME,'slct_sub_box')
    driver.execute_script("arguments[0].style.display = 'block';", select_element)
    view_port = select_element.find_element(By.CLASS_NAME,'viewport')
    containers = view_port.find_element(By.ID,'container_ddlRatifications').find_elements(By.TAG_NAME,'li')
    i = 0
    for container in containers:
        i = i - 40
        container.click()
        time.sleep(2)
        note = driver.find_element(By.ID,'desc_cntnt_main')
        driver.execute_script("arguments[0].style.display = 'block';", note)
        concept = note.text
        pdf_metadata = driver.find_element(By.CLASS_NAME,'icn_list.flt_rgt.container_ratLinks').find_element(By.ID,'link1').get_attribute('href')
        pdf_title = pdf_metadata.split('Documents/')[1]
        urllib.request.urlretrieve(pdf_metadata,f"{base_path}\\{pdf_title}")
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
        if 'Total' in file_pdf:
            import pdfplumber
            conten=[]
            with pdfplumber.open(f'{base_path}\\MetadataRatificationTotal_Dashboard.pdf') as pdf:
                text = pdf.pages
                for te in text:
                    content = te.extract_text()
                    conten.append(content)
                    content_all = ' '.join([str(elem) for elem in conten])
                    description = content_all.split('Definition:')[1].split('Rationale:')[0].replace('\n','')
                    rationale = content_all.split('Rationale:')[1].split('Concepts:')[0].replace('\n','')
        else:
            import pdfplumber
            conten=[]
            with pdfplumber.open(f'{base_path}\\MetadataRatificationStatus_Dashboard.pdf') as pdf:
                text = pdf.pages
                for te in text:
                    content = te.extract_text()
                    conten.append(content)
            content_all = ' '.join([str(elem) for elem in conten])
            description = content_all.split(' \n \nRationale')[0].replace('   ',' : ').replace('\n','').replace('  Definition  ','. \n\n Definition : ')
            rationale = content_all.split(' \n \nRationale')[1].split('Method of')[0].replace('\n','')
            periodicity = content_all.split('Periodicity')[1].split('Disaggregation ')[0].replace('\n','')
        
        data = driver.find_element(By.ID,'link5')
        data.click()
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
        filename = os.path.split(file)[1].split('_2')[0].replace('_',' ').split('UnderlyingData ')[1]
        
        df = pd.read_excel(f'{file}')
        topic = df.columns[0].split('[')[0]
        try:
            last_update = df.columns[0].split(': ')[1].replace(']','').split(' ')[0]
            last_update = datetime.datetime.strptime(last_update, "%d/%m/%Y").strftime('%Y-%m-%d')
            last_update_H = df.columns[0].split(': ')[1].replace(']','').split(' ')[1]
            last_update = last_update + 'T' + last_update_H
            source = df.iloc[201,0]
            df.columns = df.iloc[0,:]
            source_name = source.split('the ')[1].split(' (')[0]
            source_url = source.split(') ')[1]
            df = df[1:199]
            df.to_csv(f'{base_path}\\{filename}.csv',index=False)
            try:
                BodyDict = {
                    "JobPath":f"//10.30.31.77/data_collection_dump/RawData/OHCHR/{filename}.csv", #* Point to downloaded data for conversion 
                    "JsonDetails":{
                            ## Required
                            "organisation": "un-agencies",
                            "source": "OHCHR",
                            "source_description" : "The Office of the High Commissioner for Human Rights (UN Human Rights) is the leading UN entity on human rights. We represent the world's commitment to the promotion and protection of the full range of human rights and freedoms set out in the Universal Declaration of Human Rights.",
                            "source_url" : "https://indicators.ohchr.org/",
                            "table" : filename,
                            "description" : description,
                            ## Optional
                            "JobType": "JSON",
                            "CleanPush": True,
                            "Server": "str",
                            "UseJsonFormatForSQL":  False,
                            "CleanReplace":True,
                            "MergeSchema": False,
                            "tags": [{
                                "name": 'Human Rights'
                            }],
                            "additional_data_sources": [{
                                "name": source_name,
                                "url" : source_url
                            }],
                            "limitations":rationale,
                            "concept":  concept,
                            "periodicity":  periodicity,
                            "topic":  topic,
                            "created": last_update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
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
        except:
            last_update = ''
            source = df.iloc[202,0]
            df.columns = df.iloc[0,:]
            source_name = source.split('the ')[1].split(' (')[0]
            source_url = source.split(') ')[1]
            df = df[1:200]
            df.to_csv(f'{base_path}\\{filename}.csv',index=False)
            try:
                BodyDict = {
                    "JobPath":f"//10.30.31.77/data_collection_dump/RawData/OHCHR/{filename}.csv", #* Point to downloaded data for conversion 
                    "JsonDetails":{
                            ## Required
                            "organisation": "un-agencies",
                            "source": "OHCHR",
                            "source_description" : "The Office of the High Commissioner for Human Rights (UN Human Rights) is the leading UN entity on human rights. We represent the world's commitment to the promotion and protection of the full range of human rights and freedoms set out in the Universal Declaration of Human Rights.",
                            "source_url" : "https://indicators.ohchr.org/",
                            "table" : filename,
                            "description" : description,
                            ## Optional
                            "JobType": "JSON",
                            "CleanPush": True,
                            "Server": "str",
                            "UseJsonFormatForSQL":  False,
                            "CleanReplace":True,
                            "MergeSchema": False,
                            "tags": [{
                                "name": 'Human Rights'
                            }],
                            "additional_data_sources": [{
                                "name": source_name,
                                "url" : source_url
                            }],
                            "limitations":rationale,
                            "concept":  concept,
                            "periodicity":  '',
                            "topic":  topic,
                            "created": last_update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
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
        top = driver.find_element(By.ID,'rectfctn_dd')
        select_element = top.find_element(By.CLASS_NAME,'slct_sub_box')
        driver.execute_script("arguments[0].style.display = 'block';", select_element)
        view_port = select_element.find_element(By.CLASS_NAME,'viewport')
        containers = view_port.find_element(By.ID,'container_ddlRatifications')
        driver.execute_script("arguments[0].style.display = 'block';", containers)
        driver.execute_script(f"arguments[0].style.top = '{i}px';", containers)
        time.sleep(2)

execute
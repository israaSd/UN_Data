from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\FAOSTAT"
import urllib.request
import logging
import pandas as pd
from selenium.webdriver.support.ui import Select
import datetime
from re import search
from Hashing.HashScrapedData import _hashing
import zipfile
from zipfile import ZipFile
import pandas as pd
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
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/FAOSTAT/FAOSTAT_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\FAOSTAT" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()


    url = 'https://www.fao.org/faostat/en/#data/domains_table'
    driver.get(url)
    time.sleep(6)

    from selenium.common.exceptions import TimeoutException
    parent_node = driver.find_element(By.CLASS_NAME,'domain-list-container')
    childs_node = parent_node.find_elements(By.CLASS_NAME,'menu-category.instafilta-section.domain-group-list')
    tags = []
    for child in childs_node:
        # all_li = child.find_elements(By.TAG_NAME,'li')
        all_ul = child.find_elements(By.TAG_NAME,'ul')
        for ul in all_ul:
            try : 
                tag = ul.find_element(By.TAG_NAME,'h4').text
            except:
                tag = child.find_element(By.TAG_NAME,'h1').text
            all_li = ul.find_elements(By.TAG_NAME,'li')
            for li in all_li:
                tags.append(tag)
    all_li = parent_node.find_elements(By.TAG_NAME,'li') 
    for i in range(len(all_li)):
        if i in [9,10,49] : 
            tag = tags[i]
            try:
                time.sleep(2)
                dataset = driver.find_element(By.CLASS_NAME,'domain-list-container').find_elements(By.TAG_NAME,'a')[i]
                title = dataset.text
                # time.sleep(4)
                dataset.click()
                timeout = 15
                try:
                    element_present = EC.presence_of_element_located((By.XPATH,"//*[contains(text(), 'All Data')]"))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    print("Timed out waiting for page to load ALL DATA NORMALIZED")
                # print(metadata)
                # time.sleep(10)
                # bulk = driver.find_element(By.CLASS_NAME,'panel-footer.amber.lighten-4')
                # download = bulk.find_elements(By.TAG_NAME,'li')[1]
                download = driver.find_element(By.XPATH,"//*[contains(text(), 'All Data')]")
                download.click()
                time.sleep(2)
                # file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                def latest_download_file():
                    path = base_path
                    os.chdir(path)
                    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                    newest = files[-1]

                    return newest

                fileends = "crdownload"
                while "crdownload" == fileends:
                    time.sleep(5) 
                    newest_file = latest_download_file()
                    if "crdownload" in newest_file:
                        fileends = "crdownload"
                        # time.sleep(5)
                    else:
                        fileends = "None"
                latest_download_file()    
                time.sleep(2)
                info_data = driver.find_element(By.CLASS_NAME,'nav.nav-tabs')
                metadata = info_data.find_elements(By.TAG_NAME,'li')[2].find_element(By.TAG_NAME,'a')
                metadata.click()
                timeout = 15
                try:
                    element_present = EC.presence_of_element_located((By.CLASS_NAME,"fs-metadata-printable-content"))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    print("Timed out waiting for page to load METADATA")
                time.sleep(1)
                try:
                    all_metadata = driver.find_element(By.CLASS_NAME,"fs-metadata-printable-content")
                    rows = all_metadata.find_elements(By.CLASS_NAME,'row.fs-metadata-row')
                    for row in rows:
                        try:
                            if 'last update' in row.text:
                                update = row.find_element(By.CLASS_NAME,'col-sm-9').text
                                try:
                                    update = datetime.datetime.strptime(update, "%B %Y").strftime('%Y-%m-%d')       
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d/%m/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try : 
                                    update = datetime.datetime.strptime(update, "%d %B %Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d.%m.%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d-%m-%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%m/%d/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                        except:
                            update = ''
                        try:
                            if 'last certified' in row.text:
                                created = row.find_element(By.CLASS_NAME,'col-sm-9').text
                                try:
                                    created = datetime.datetime.strptime(created, "%B %Y").strftime('%Y-%m-%d')       
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d/%m/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try : 
                                    created = datetime.datetime.strptime(created, "%d %B %Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d.%m.%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d-%m-%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                # if created == '':
                                #     created = update
                        except:
                            created = update
                        try:
                            if 'description' in row.text:
                                description  = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            description = ''
                        try:
                            if 'Sector coverage' in row.text:
                                limitation = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            limitation = ''
                        try:
                            if 'concepts' in row.text:
                                concept = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            concept = '' 
                        try:
                            if 'Periodicity' in row.text:
                                periodicity = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            periodicity = '' 
                        try:
                            if 'Source data' in row.text:
                                source = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            source = ''
                except:
                    pass
                if created == '':
                    created = update
                if update == '':
                    update = created
                time.sleep(1)
                flags = []
                for zipfiles in os.listdir(base_path):
                    if zipfiles.endswith('zip'):
                        zip = zipfile.ZipFile(f'{base_path}\\{zipfiles}')
                        # with ZipFile(f'{base_path}\\{zipfiles}', 'r') as zipObject:
                        files = zip.namelist()
                        try:
                            f = zip.open(files[3])
                            df = pd.read_csv(f)
                            for i in range(len(df.index)):
                                f = df.iloc[i].to_list()
                                f = [' : '.join(map(str,f))]
                                flags.append(f[0])
                            flag = [' , '.join(map(str,flags))]
                            flag = 'Flag : Description' + ' , ' + flag[0]
                        except:
                            flag = ''
                        with zipfile.ZipFile(f'{base_path}\\{zipfiles}') as z:
                            with z.open(files[0]) as zf, open(f'{base_path}\\{files[0]}', 'wb') as f:
                                shutil.copyfileobj(zf, f)
                
                file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                filename = os.path.split(file)[1]
                table_name = filename.split('.')[0]
                # print(tag)
                # print(title)
                # print(filename)
                # print(flag)
                time.sleep(1)
                driver.get(url)
                time.sleep(2)
                try:
                    BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/FAOSTAT/{table_name}.csv", #* Point to downloaded data for conversion 
                        "JsonDetails":{
                                ## Required
                                "organisation": "un-agencies",
                                "source": "FAO",
                                "source_description" : "The Food and Agriculture Organization (FAO) is a specialized agency of the United Nations that leads international efforts to defeat hunger.",
                                "source_url" : "https://www.fao.org/faostat/en/#data/LC/metadata",
                                "table" : table_name,
                                "description" : flag + '\n' + description,
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
                                    "name": source
                                }],
                                "limitations":limitation,
                                "concept":  concept,
                                "periodicity":  periodicity,
                                "topic":  title,
                                "created": created,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
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
            except:
                driver.get(url)
                time.sleep(2)


        else:
            tag = tags[i]
            # try:
            time.sleep(2)
            dataset = driver.find_element(By.CLASS_NAME,'domain-list-container').find_elements(By.TAG_NAME,'a')[i]
            title = dataset.text
            time.sleep(4)
            dataset.click()
            timeout = 15
            try:
                element_present = EC.presence_of_element_located((By.XPATH,"//*[contains(text(), 'All Data Normalized')]"))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load ALL DATA NORMALIZED")
            # print(metadata)
            # time.sleep(10)
            # bulk = driver.find_element(By.CLASS_NAME,'panel-footer.amber.lighten-4')
            # download = bulk.find_elements(By.TAG_NAME,'li')[1]
            try:
                download = driver.find_element(By.XPATH,"//*[contains(text(), 'All Data Normalized')]")
                download.click()
                time.sleep(2)
                # file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                def latest_download_file():
                    path = base_path
                    os.chdir(path)
                    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                    newest = files[-1]

                    return newest

                fileends = "crdownload"
                while "crdownload" == fileends:
                    time.sleep(5) 
                    newest_file = latest_download_file()
                    if "crdownload" in newest_file:
                        fileends = "crdownload"
                        # time.sleep(5)
                    else:
                        fileends = "None"
                latest_download_file()    
                time.sleep(2)
                info_data = driver.find_element(By.CLASS_NAME,'nav.nav-tabs')
                metadata = info_data.find_elements(By.TAG_NAME,'li')[2].find_element(By.TAG_NAME,'a')
                metadata.click()
                timeout = 15
                try:
                    element_present = EC.presence_of_element_located((By.CLASS_NAME,"fs-metadata-printable-content"))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    print("Timed out waiting for page to load METADATA")
                time.sleep(1)
                try:
                    all_metadata = driver.find_element(By.CLASS_NAME,"fs-metadata-printable-content")
                    rows = all_metadata.find_elements(By.CLASS_NAME,'row.fs-metadata-row')
                    for row in rows:
                        try:
                            if 'last update' in row.text:
                                update = row.find_element(By.CLASS_NAME,'col-sm-9').text
                                try:
                                    update = datetime.datetime.strptime(update, "%B %Y").strftime('%Y-%m-%d')       
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d/%m/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try : 
                                    update = datetime.datetime.strptime(update, "%d %B %Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d.%m.%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%d-%m-%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    update = datetime.datetime.strptime(update, "%m/%d/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                        except:
                            update = ''
                        try:
                            if 'last certified' in row.text:
                                created = row.find_element(By.CLASS_NAME,'col-sm-9').text
                                try:
                                    created = datetime.datetime.strptime(created, "%B %Y").strftime('%Y-%m-%d')       
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d/%m/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try : 
                                    created = datetime.datetime.strptime(created, "%d %B %Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d.%m.%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%d-%m-%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                try :
                                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                # if created == '':
                                #     created = update
                        except:
                            created = update
                        try:
                            if 'description' in row.text:
                                description  = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            description = ''
                        try:
                            if 'Sector coverage' in row.text:
                                limitation = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            limitation = ''
                        try:
                            if 'concepts' in row.text:
                                concept = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            concept = '' 
                        try:
                            if 'Periodicity' in row.text:
                                periodicity = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            periodicity = '' 
                        try:
                            if 'Source data' in row.text:
                                source = row.find_element(By.CLASS_NAME,'col-sm-9').text
                        except:
                            source = ''
                except:
                    pass
                if created == '':
                    created = update
                if update == '':
                    update = created
                time.sleep(1)
                flags = []
                for zipfiles in os.listdir(base_path):
                    if zipfiles.endswith('zip'):
                        zip = zipfile.ZipFile(f'{base_path}\\{zipfiles}')
                        # with ZipFile(f'{base_path}\\{zipfiles}', 'r') as zipObject:
                        files = zip.namelist()
                        try:
                            f = zip.open(files[2])
                            df = pd.read_csv(f)
                            for i in range(len(df.index)):
                                f = df.iloc[i].to_list()
                                f = [' : '.join(map(str,f))]
                                flags.append(f[0])
                            flag = [' , '.join(map(str,flags))]
                            flag = 'Flag : Description' + ' , ' + flag[0]
                        except:
                            flag = ''
                        with zipfile.ZipFile(f'{base_path}\\{zipfiles}') as z:
                            with z.open(files[0]) as zf, open(f'{base_path}\\{files[0]}', 'wb') as f:
                                shutil.copyfileobj(zf, f)

                file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                filename = os.path.split(file)[1]
                table_name = filename.split('.')[0]
                # print(tag)
                # print(title)
                # print(filename)
                # print(flag)
                time.sleep(1)
                driver.get(url)
                time.sleep(2)
                try:
                    BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/FAOSTAT/{table_name}.csv", #* Point to downloaded data for conversion 
                        "JsonDetails":{
                                ## Required
                                "organisation": "un-agencies",
                                "source": "FAO",
                                "source_description" : "The Food and Agriculture Organization (FAO) is a specialized agency of the United Nations that leads international efforts to defeat hunger.",
                                "source_url" : "https://www.fao.org/faostat/en/#data/LC/metadata",
                                "table" : table_name,
                                "description" : flag + '\n' + description,
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
                                    "name": source
                                }],
                                "limitations":limitation,
                                "concept":  concept,
                                "periodicity":  periodicity,
                                "topic":  title,
                                "created": created,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
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
            except:
                print('not raise')
                driver.get(url)
                time.sleep(2)

# execute()

from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\IRENA"
import urllib.request
import logging
import pandas as pd
from selenium.webdriver.support.ui import Select
import datetime
from re import search
from functools import reduce
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
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/IRENA/IRENA.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\IRENA" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    # chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    BodyDict = {
            "JobPath":"", #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "third-parties",
                    "source": "IRENA",
                    "source_description" : "The International Renewable Energy Agency (IRENA) is an intergovernmental organisation that supports countries in their transition to a sustainable energy future, and serves as the principal platform for international co-operation, a centre of excellence, and a repository of policy, technology, resource and financial knowledge on renewable energy. IRENA promotes the widespread adoption and sustainable use of all forms of renewable energy, including bioenergy, geothermal, hydropower, ocean, solar and wind energy, in the pursuit of sustainable development, energy access, energy security and low-carbon economic growth and prosperity.",
                    "source_url" : "https://www.irena.org/Data",
                    "table" : '',
                    "description" : "",
                    ## Optional
                    "JobType": "JSON",
                    "CleanPush": True,
                    "Server": "str",
                    "UseJsonFormatForSQL":  False,
                    "CleanReplace":True,
                    "MergeSchema": False,
                    "tags": [{
                        "name": ''
                    }],
                    "additional_data_sources": [{       
                            "name": '',        
                            "url": ''  ## this object will be ignored if "name" is empty    }
                    }],
                    "limitations":"",
                    "concept":  "",
                    "periodicity":  "",
                    "topic":  "",
                    "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": "",                #* ""               ""                  ""              ""
                    "TriggerTalend" :  False,    #* initialise to True for production
                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData" #* initialise as empty string for production.
                }
            }

    url = 'https://www.irena.org/Data'
    driver.get(url)
    time.sleep(1)
    from selenium.common.exceptions import TimeoutException
    timeout = 15
    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load element")
    view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
    time.sleep(2)
    view_data_by_topic[1].click()
    time.sleep(1)
    list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
    tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
    for i in range(len(tags)):
        if i in [0]:
            tag = tags[i].text
            view_data_by_topic[1].click()
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[1]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            div = center.find_element(By.ID,'view6762280515476473387_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            title = filename.split('.')[0]
            xls = pd.ExcelFile(f'{base_path}\\{filename}')
            sheets = xls.sheet_names
            df_desc = pd.read_excel(f'{base_path}\\{filename}',sheets[1])
            desc = df_desc.iloc[3,0] + ' ' + df_desc.iloc[4,0]
            source = df_desc.iloc[0,0]
            url_source = df_desc.iloc[1,0]
            df = df_desc = pd.read_excel(f'{base_path}\\{filename}',sheets[0])
            df.to_csv(f'{base_path}\\{title}.csv',index=False)
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{title}.csv"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = desc
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_source
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        elif tags[i].text == 'Costs':
            tag = tags[i].text
            view_data_by_topic[1].click()
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[0]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            note = center.find_element(By.CLASS_NAME,'tab-textRegion.tab-widget')
            desc_note = note.find_elements(By.TAG_NAME,'span')[2].text
            source = note.find_elements(By.TAG_NAME,'span')[4].text
            url_source = note.find_elements(By.TAG_NAME,'span')[5].text
            div = center.find_element(By.ID,'view11151608863636357857_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            time.sleep(6)
            xls = pd.ExcelFile(f'{base_path}\\{filename}')
            sheets = xls.sheet_names
            df = pd.read_excel(f'{base_path}\\{filename}','Table 5.2')
            title = df.columns[0]
            desc = df.iloc[19,0]
            df = df.iloc[4:19,:]
            df.columns = ['Country','Parabolic trough collectors (2020 USD/kWh)' , 'Solar tower (2020 USD/kWh)']
            df.to_csv(f'{base_path}\\{title}.csv',index=False)
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{title}.csv"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["description"] = desc + ' ' + desc_note
                BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_source
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        elif tags[i].text == 'Energy Transition':
            tag = tags[i].text
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[1]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            div = center.find_element(By.ID,'view16206229265905950579_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            title = filename.split('.')[0]
            xls = pd.ExcelFile(f'{base_path}\\{filename}')
            sheets = xls.sheet_names
            df_desc = pd.read_excel(f'{base_path}\\{filename}',sheets[1])
            desc = df_desc.iloc[28,0] + ' ' + df_desc.iloc[29,0] + ' ' + df_desc.iloc[30,0] + ' ' + df_desc.iloc[31,0] + ' ' + df_desc.iloc[32,0] + ' ' + df_desc.iloc[33,0] + ' ' + df_desc.iloc[34,0] + ' ' + df_desc.iloc[35,0]
            source = df_desc.iloc[23,0]
            url_source = df_desc.iloc[25,0]
            df = df_desc = pd.read_excel(f'{base_path}\\{filename}',sheets[0])
            df.to_csv(f'{base_path}\\{title}.csv',index=False)
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{title}.csv"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = desc
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_source
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        elif tags[i].text == 'Finance and Investment':
            tag = tags[i].text
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[1]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            div = center.find_element(By.ID,'view7734379744678548899_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            title = filename.split('.')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{filename}"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = title
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        elif tags[i].text == 'Innovation and Technology':
            tag = tags[i].text
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[1]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            div = center.find_element(By.ID,'view3912306690392202374_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            title = filename.split('.')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            xls = pd.ExcelFile(f'{base_path}\\{filename}')
            sheets = xls.sheet_names
            df = pd.read_excel(f'{base_path}\\{filename}',sheets[1])
            df_desc = pd.read_excel(f'{base_path}\\{filename}',sheets[0])
            desc = df_desc.iloc[17,1] +  ' ' + df_desc.iloc[18,1] +  ' ' + df_desc.iloc[19,1]
            source = df_desc.iloc[22,1].split('.')[0]
            url_source = df_desc.iloc[22,1].split(':')[1]
            df.to_csv(f'{base_path}\\{title}.csv',index=False)
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{title}.csv"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = desc
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_source
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        elif tags[i].text == 'Renewable Energy Balances':
            tag = tags[i].text
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            tags.click()
            time.sleep(2)
            dataset = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.CLASS_NAME,'m-SideMenu__container').find_elements(By.TAG_NAME,'li')[1]
            # topic = dataset.text
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'m-Iframe')
            div = content.find_element(By.TAG_NAME,'iframe')
            html = div.find_element(By.XPATH,'/html')
            html.click()
            time.sleep(1)
            driver.switch_to.frame(div)
            center = driver.find_element(By.ID,'centeringContainer')
            div = center.find_element(By.ID,'view6882849831517330334_7506702030957726925')
            canvas = div.find_elements(By.TAG_NAME,'canvas')
            canvas[1].click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(1)
            page_source = driver.page_source               
            soup = BeautifulSoup(page_source, 'html.parser')
            filename = soup.find('script').text.split('"FileName":"')[1].split('","')[0]
            title = filename.split('.')[0]
            link = soup.find('script').text.split('"FileGetUrl":"')[1].split("\\")[0]
            driver.get(link)
            time.sleep(1)
            driver.close()
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{filename}"
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = desc
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["topic"] = tag
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
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)
            driver.back()
            time.sleep(3)
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            view_data_by_topic = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            view_data_by_topic[1].click()
            time.sleep(1)
            list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')

    downloads = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
    for j in downloads:
        if j.text == 'Downloads':
            time.sleep(2)
            j.click()
            time.sleep(1)
            list_data = driver.find_elements(By.CLASS_NAME,'m-SideMenu__container')
            # topics = list_data[1].find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
            # download[2].click()
            # list_data = driver.find_element(By.CLASS_NAME,'m-SideMenu__container')
            # tags = list_data.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'button')
            # tags.click()
            time.sleep(2)
            dataset = list_data[10].find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0]
            link_dataset = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link_dataset)
            time.sleep(2)
            button_onlineData = driver.find_element(By.CLASS_NAME,'c-RichText ').find_element(By.CLASS_NAME,'c-Button').get_attribute('href')
            # print(button_onlineData)
            driver.get(button_onlineData)
            tag = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].text
            power_capacity_generation = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_element(By.CLASS_NAME,'AspNet-TreeView-Expand')
            power_capacity_generation.click()
            list_pcg = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_elements(By.CLASS_NAME,'AspNet-TreeView-Leaf')
            for p in range(len(list_pcg)):
                if p in [0]:
                    list_pcg = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_elements(By.CLASS_NAME,'AspNet-TreeView-Leaf')
                    topic = list_pcg[p].text
                    installed_mw = list_pcg[p].find_element(By.TAG_NAME,'a').get_attribute('href')
                    title = list_pcg[p].find_element(By.TAG_NAME,'span').text.replace('/area','')
                    col_0 = title.split(',')[0]
                    driver.get(installed_mw)
                    about_table = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
                    about_table.click()
                    op = driver.find_element(By.ID,'AboutTable').find_elements(By.TAG_NAME,'a')
                    op[0].click()
                    content = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_divInformation')
                    created = content.find_element(By.CLASS_NAME,'information_lastupdated_value').text
                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                    desc_note = content.find_element(By.CLASS_NAME,'information_unit_value').text
                    try:
                        periodicity = content.find_element(By.CLASS_NAME,'information_updatefrequency_value').text
                    except:
                        periodicity = ''
                    last_update = content.find_element(By.CLASS_NAME,'information_nextupdate_value').text
                    last_update = datetime.datetime.strptime(last_update, "%m/%d/%Y").strftime('%Y-%m-%d')
                    source = content.find_element(By.CLASS_NAME,'information_source_value').text
                    op[2].click()
                    footnote = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_SelectionFootnotes').find_element(By.CLASS_NAME,'footnote_note_value ').text
                    table_filter = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0].find_element(By.TAG_NAME,'a')
                    table_filter.click()
                    arab_countries = ['Algeria', 'Bahrain', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan', 'Kuwait', 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Sudan' , 'Syrian Arab Republic' , 'Tunisia', 'United Arab Emirates', 'Yemen']
                    from selenium.webdriver.support.ui import Select
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    import csv
                    country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                    select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                    driver.execute_script("arguments[0].style.display = 'block';", select_element)
                    select_element.is_displayed()
                    select = Select(select_element)
                    option_list = select.options
                    for i in arab_countries:
                        country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                        select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                        driver.execute_script("arguments[0].style.display = 'block';", select_element)
                        select_element.is_displayed()
                        select = Select(select_element)
                        option_list = select.options
                        select.select_by_visible_text(i)
                        technology = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_EventButtons')
                        tech_all = technology.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        tech_all.click()
                        grid_conn = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_EventButtons')
                        grid_all = grid_conn.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        grid_all.click()
                        year = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_EventButtons')
                        all_year = year.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        all_year.click()
                        continu = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_ButtonViewTable')
                        continu.click()
                        page_source = driver.page_source               
                        soup = BeautifulSoup(page_source, 'html.parser')
                        table = soup.find(id='ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable')
                        rows = table.find_all('tr')
                        l = []
                        for tr in rows[3:]:
                            cells = tr.find_all(['td','th'])
                            cells_text = [cell.get_text(strip=True) for cell in cells]
                            l.append(cells_text)
                        df = pd.DataFrame(l,columns=["year", "B"])
                        df_1 = df[2:24]
                        df_1.columns = ["year",f"{df.iloc[0,0]} {df.iloc[1,0]}"]
                        df_2 = df[25:47]
                        df_2.columns = ["year",f"{df.iloc[0,0]} {df.iloc[24,0]}"]
                        df_3 = df[49:71]
                        df_3.columns = ["year",f"{df.iloc[47,0]} {df.iloc[48,0]}"]
                        df_4 = df[72:94]
                        df_4.columns = ["year",f"{df.iloc[47,0]} {df.iloc[71,0]}"]
                        df_5 = df[96:118]
                        df_5.columns = ["year",f"{df.iloc[94,0]} {df.iloc[95,0]}"]
                        df_6 = df[119:141]
                        df_6.columns = ["year",f"{df.iloc[94,0]} {df.iloc[118,0]}"]
                        df_7 = df[143:165]
                        df_7.columns = ["year",f"{df.iloc[141,0]} {df.iloc[142,0]}"]
                        df_8 = df[166:188]
                        df_8.columns = ["year",f"{df.iloc[141,0]} {df.iloc[165,0]}"]
                        df_9 = df[190:212]
                        df_9.columns = ["year",f"{df.iloc[188,0]} {df.iloc[189,0]}"]
                        df_10 = df[213:235]
                        df_10.columns = ["year",f"{df.iloc[188,0]} {df.iloc[212,0]}"]
                        df_11 = df[237:259]
                        df_11.columns = ["year",f"{df.iloc[235,0]} {df.iloc[236,0]}"]
                        df_12 = df[260:282]
                        df_12.columns = ["year",f"{df.iloc[235,0]} {df.iloc[259,0]}"]
                        df_13 = df[284:306]
                        df_13.columns = ["year",f"{df.iloc[282,0]} {df.iloc[283,0]}"]
                        df_14 = df[307:329]
                        df_14.columns = ["year",f"{df.iloc[282,0]} {df.iloc[306,0]}"]
                        df_15 = df[331:353]
                        df_15.columns = ["year",f"{df.iloc[329,0]} {df.iloc[330,0]}"]
                        df_16 = df[354:376]
                        df_16.columns = ["year",f"{df.iloc[329,0]} {df.iloc[353,0]}"]
                        df_17 = df[378:400]
                        df_17.columns = ["year",f"{df.iloc[376,0]} {df.iloc[377,0]}"]
                        df_18 = df[401:423]
                        df_18.columns = ["year",f"{df.iloc[376,0]} {df.iloc[400,0]}"]
                        df_19 = df[425:447]
                        df_19.columns = ["year",f"{df.iloc[423,0]} {df.iloc[424,0]}"]
                        df_20 = df[448:470]
                        df_20.columns = ["year",f"{df.iloc[423,0]} {df.iloc[447,0]}"]
                        df_21 = df[472:494]
                        df_21.columns = ["year",f"{df.iloc[470,0]} {df.iloc[471,0]}"]
                        df_22 = df[495:517]
                        df_22.columns = ["year",f"{df.iloc[470,0]} {df.iloc[494,0]}"]
                        df_23 = df[519:541]
                        df_23.columns = ["year",f"{df.iloc[517,0]} {df.iloc[518,0]}"]
                        df_24 = df[542:564]
                        df_24.columns = ["year",f"{df.iloc[517,0]} {df.iloc[541,0]}"]
                        df_25 = df[566:588]
                        df_25.columns = ["year",f"{df.iloc[564,0]} {df.iloc[565,0]}"]
                        df_26 = df[589:611]
                        df_26.columns = ["year",f"{df.iloc[564,0]} {df.iloc[588,0]}"]
                        df_27 = df[613:635]
                        df_27.columns = ["year",f"{df.iloc[611,0]} {df.iloc[612,0]}"]
                        df_28 = df[636:658]
                        df_28.columns = ["year",f"{df.iloc[611,0]} {df.iloc[635,0]}"]
                        df_29 = df[660:682]
                        df_29.columns = ["year",f"{df.iloc[658,0]} {df.iloc[659,0]}"]
                        df_30 = df[683:705]
                        df_30.columns = ["year",f"{df.iloc[658,0]} {df.iloc[682,0]}"]
                        df_31 = df[707:729]
                        df_31.columns = ["year",f"{df.iloc[705,0]} {df.iloc[706,0]}"]
                        df_32 = df[730:752]
                        df_32.columns = ["year",f"{df.iloc[705,0]} {df.iloc[729,0]}"]
                        df_33 = df[754:776]
                        df_33.columns = ["year",f"{df.iloc[752,0]} {df.iloc[753,0]}"]
                        df_34 = df[777:799]
                        df_34.columns = ["year",f"{df.iloc[752,0]} {df.iloc[776,0]}"]
                        df_35 = df[801:823]
                        df_35.columns = ["year",f"{df.iloc[799,0]} {df.iloc[800,0]}"]
                        df_36 = df[824:846]
                        df_36.columns = ["year",f"{df.iloc[799,0]} {df.iloc[823,0]}"]
                        df_37 = df[848:870]
                        df_37.columns = ["year",f"{df.iloc[846,0]} {df.iloc[847,0]}"]
                        df_38 = df[871:893]
                        df_38.columns = ["year",f"{df.iloc[846,0]} {df.iloc[870,0]}"]
                        pdList = [df_1, df_2, df_3 , df_4 , df_5 , df_6 , df_7 , df_8 , df_9 , df_10 , df_11 , df_12 , df_13 , df_14 , df_15 , df_16 , df_17 , df_18 , df_19 , df_20 , df_21 , df_22 , df_23 , df_24 , df_25 , df_26 , df_27 , df_28 , df_29 , df_30 , df_31 , df_32 , df_33 , df_34 , df_35 , df_36 , df_37 , df_38] 
                        df = reduce(lambda  left,right: pd.merge(left,right,on=['year'],how='outer'), pdList) 
                        df.to_csv(f'{base_path}\\{i} - {title}.csv', index=False)
                        try:
                            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{i} - {title}.csv"
                            BodyDict["JsonDetails"]["table"] = f'{i} - {title}'
                            BodyDict["JsonDetails"]["description"] = desc_note + ' ' + footnote
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["topic"] = topic
                            BodyDict["JsonDetails"]["last_modified"] =  last_update
                            BodyDict["JsonDetails"]["created"] =  created
                            BodyDict["JsonDetails"]["periodicity"] = periodicity
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
                        r_country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_EventButtons')
                        remove_country = r_country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_DeselectAllButton')
                        remove_country.click()
                    driver.back()
                    try:
                        element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                        WebDriverWait(driver, timeout).until(element_present)
                    except TimeoutException:
                        print("Timed out waiting for page to load element")
                    tags = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')

                if p in [1]:
                    list_pcg = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_elements(By.CLASS_NAME,'AspNet-TreeView-Leaf')
                    topic = list_pcg[p].text
                    installed_mw = list_pcg[p].find_element(By.TAG_NAME,'a').get_attribute('href')
                    title = list_pcg[p].find_element(By.TAG_NAME,'span').text.replace('/area','')
                    col_0 = title.split(',')[0]
                    driver.get(installed_mw)
                    about_table = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
                    about_table.click()
                    op = driver.find_element(By.ID,'AboutTable').find_elements(By.TAG_NAME,'a')
                    op[0].click()
                    content = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_divInformation')
                    created = content.find_element(By.CLASS_NAME,'information_lastupdated_value').text
                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                    desc_note = content.find_element(By.CLASS_NAME,'information_unit_value').text
                    try:
                        periodicity = content.find_element(By.CLASS_NAME,'information_updatefrequency_value').text
                    except:
                        periodicity = ''
                    last_update = content.find_element(By.CLASS_NAME,'information_nextupdate_value').text
                    last_update = datetime.datetime.strptime(last_update, "%m/%d/%Y").strftime('%Y-%m-%d')
                    source = content.find_element(By.CLASS_NAME,'information_source_value').text
                    op[2].click()
                    footnote = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_SelectionFootnotes').find_element(By.CLASS_NAME,'footnote_note_value ').text
                    table_filter = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0].find_element(By.TAG_NAME,'a')
                    table_filter.click()
                    arab_countries = ['Algeria', 'Bahrain', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan', 'Kuwait', 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Sudan' , 'Syrian Arab Republic' , 'Tunisia', 'United Arab Emirates', 'Yemen']
                    from selenium.webdriver.support.ui import Select
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    import csv
                    country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                    select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                    driver.execute_script("arguments[0].style.display = 'block';", select_element)
                    select_element.is_displayed()
                    select = Select(select_element)
                    option_list = select.options
                    for i in arab_countries:
                        country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                        select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                        driver.execute_script("arguments[0].style.display = 'block';", select_element)
                        select_element.is_displayed()
                        select = Select(select_element)
                        option_list = select.options
                        select.select_by_visible_text(i)
                        technology = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_EventButtons')
                        tech_all = technology.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        tech_all.click()
                        grid_conn = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_EventButtons')
                        grid_all = grid_conn.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        grid_all.click()
                        year = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_EventButtons')
                        all_year = year.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        all_year.click()
                        continu = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_ButtonViewTable')
                        continu.click()
                        page_source = driver.page_source               
                        soup = BeautifulSoup(page_source, 'html.parser')
                        table = soup.find(id='ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable')
                        rows = table.find_all('tr')
                        l = []
                        for tr in rows[3:]:
                            cells = tr.find_all(['td','th'])
                            cells_text = [cell.get_text(strip=True) for cell in cells]
                            l.append(cells_text)
                        df = pd.DataFrame(l,columns=["year", "B"])
                        df_1 = df[2:23]
                        df_1.columns = ["year",f"{df.iloc[0,0]} {df.iloc[1,0]}"]
                        df_2 = df[24:45]
                        df_2.columns = ["year",f"{df.iloc[0,0]} {df.iloc[23,0]}"]
                        df_3 = df[47:68]
                        df_3.columns = ["year",f"{df.iloc[45,0]} {df.iloc[46,0]}"]
                        df_4 = df[69:90]
                        df_4.columns = ["year",f"{df.iloc[45,0]} {df.iloc[68,0]}"]
                        df_5 = df[92:113]
                        df_5.columns = ["year",f"{df.iloc[90,0]} {df.iloc[91,0]}"]
                        df_6 = df[114:135]
                        df_6.columns = ["year",f"{df.iloc[90,0]} {df.iloc[113,0]}"]
                        df_7 = df[137:158]
                        df_7.columns = ["year",f"{df.iloc[135,0]} {df.iloc[136,0]}"]
                        df_8 = df[159:180]
                        df_8.columns = ["year",f"{df.iloc[135,0]} {df.iloc[158,0]}"]
                        df_9 = df[182:203]
                        df_9.columns = ["year",f"{df.iloc[180,0]} {df.iloc[181,0]}"]
                        df_10 = df[204:225]
                        df_10.columns = ["year",f"{df.iloc[180,0]} {df.iloc[203,0]}"]
                        df_11 = df[227:248]
                        df_11.columns = ["year",f"{df.iloc[225,0]} {df.iloc[226,0]}"]
                        df_12 = df[249:270]
                        df_12.columns = ["year",f"{df.iloc[225,0]} {df.iloc[248,0]}"]
                        df_13 = df[272:293]
                        df_13.columns = ["year",f"{df.iloc[270,0]} {df.iloc[271,0]}"]
                        df_14 = df[294:315]
                        df_14.columns = ["year",f"{df.iloc[270,0]} {df.iloc[293,0]}"]
                        df_15 = df[317:338]
                        df_15.columns = ["year",f"{df.iloc[315,0]} {df.iloc[316,0]}"]
                        df_16 = df[339:360]
                        df_16.columns = ["year",f"{df.iloc[315,0]} {df.iloc[338,0]}"]
                        df_17 = df[362:383]
                        df_17.columns = ["year",f"{df.iloc[360,0]} {df.iloc[361,0]}"]
                        df_18 = df[384:405]
                        df_18.columns = ["year",f"{df.iloc[360,0]} {df.iloc[383,0]}"]
                        df_19 = df[407:428]
                        df_19.columns = ["year",f"{df.iloc[405,0]} {df.iloc[406,0]}"]
                        df_20 = df[429:450]
                        df_20.columns = ["year",f"{df.iloc[405,0]} {df.iloc[428,0]}"]
                        df_21 = df[452:473]
                        df_21.columns = ["year",f"{df.iloc[450,0]} {df.iloc[451,0]}"]
                        df_22 = df[474:495]
                        df_22.columns = ["year",f"{df.iloc[450,0]} {df.iloc[495,0]}"]
                        df_23 = df[497:518]
                        df_23.columns = ["year",f"{df.iloc[495,0]} {df.iloc[496,0]}"]
                        df_24 = df[519:540]
                        df_24.columns = ["year",f"{df.iloc[495,0]} {df.iloc[518,0]}"]
                        df_25 = df[542:563]
                        df_25.columns = ["year",f"{df.iloc[540,0]} {df.iloc[541,0]}"]
                        df_26 = df[564:585]
                        df_26.columns = ["year",f"{df.iloc[540,0]} {df.iloc[563,0]}"]
                        df_27 = df[587:608]
                        df_27.columns = ["year",f"{df.iloc[585,0]} {df.iloc[586,0]}"]
                        df_28 = df[609:630]
                        df_28.columns = ["year",f"{df.iloc[585,0]} {df.iloc[608,0]}"]
                        df_29 = df[632:653]
                        df_29.columns = ["year",f"{df.iloc[630,0]} {df.iloc[631,0]}"]
                        df_30 = df[654:675]
                        df_30.columns = ["year",f"{df.iloc[630,0]} {df.iloc[653,0]}"]
                        df_31 = df[677:698]
                        df_31.columns = ["year",f"{df.iloc[675,0]} {df.iloc[676,0]}"]
                        df_32 = df[699:720]
                        df_32.columns = ["year",f"{df.iloc[675,0]} {df.iloc[698,0]}"]
                        df_33 = df[722:743]
                        df_33.columns = ["year",f"{df.iloc[720,0]} {df.iloc[721,0]}"]
                        df_34 = df[744:765]
                        df_34.columns = ["year",f"{df.iloc[720,0]} {df.iloc[743,0]}"]
                        df_35 = df[767:788]
                        df_35.columns = ["year",f"{df.iloc[765,0]} {df.iloc[766,0]}"]
                        df_36 = df[789:810]
                        df_36.columns = ["year",f"{df.iloc[765,0]} {df.iloc[788,0]}"]
                        df_37 = df[812:833]
                        df_37.columns = ["year",f"{df.iloc[810,0]} {df.iloc[811,0]}"]
                        df_38 = df[834:855]
                        df_38.columns = ["year",f"{df.iloc[810,0]} {df.iloc[833,0]}"]
                        pdList = [df_1, df_2, df_3 , df_4 , df_5 , df_6 , df_7 , df_8 , df_9 , df_10 , df_11 , df_12 , df_13 , df_14 , df_15 , df_16 , df_17 , df_18 , df_19 , df_20 , df_21 , df_22 , df_23 , df_24 , df_25 , df_26 , df_27 , df_28 , df_29 , df_30 , df_31 , df_32 , df_33 , df_34 , df_35 , df_36 , df_37 , df_38] 
                        df = reduce(lambda  left,right: pd.merge(left,right,on=['year'],how='outer'), pdList)
                        df.to_csv(f'{base_path}\\{i} - {title}.csv', index=False)
                        try:
                            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{i} - {title}.csv"
                            BodyDict["JsonDetails"]["table"] = f'{i} - {title}'
                            BodyDict["JsonDetails"]["description"] = desc_note + ' ' + footnote
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["topic"] = topic
                            BodyDict["JsonDetails"]["last_modified"] =  last_update
                            BodyDict["JsonDetails"]["created"] =  created
                            BodyDict["JsonDetails"]["periodicity"] = periodicity
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
                        r_country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_EventButtons')
                        remove_country = r_country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_DeselectAllButton')
                        remove_country.click()
                    driver.back()
                    try:
                        element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                        WebDriverWait(driver, timeout).until(element_present)
                    except TimeoutException:
                        print("Timed out waiting for page to load element")
                    tags = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')

                elif p in [2,3]:
                    list_pcg = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_elements(By.CLASS_NAME,'AspNet-TreeView-Leaf')
                    topic = list_pcg[p].text
                    installed_mw = list_pcg[p].find_element(By.TAG_NAME,'a').get_attribute('href')
                    title = list_pcg[p].find_element(By.TAG_NAME,'span').text.replace('/area','').replace('Region/','')
                    col_0 = title.split(',')[0]
                    driver.get(installed_mw)
                    about_table = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
                    about_table.click()
                    op = driver.find_element(By.ID,'AboutTable').find_elements(By.TAG_NAME,'a')
                    op[0].click()
                    content = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_divInformation')
                    created = content.find_element(By.CLASS_NAME,'information_lastupdated_value').text
                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                    desc_note = content.find_element(By.CLASS_NAME,'information_unit_value').text
                    try:
                        periodicity = content.find_element(By.CLASS_NAME,'information_updatefrequency_value').text
                    except:
                        pass
                    last_update = content.find_element(By.CLASS_NAME,'information_nextupdate_value').text
                    last_update = datetime.datetime.strptime(last_update, "%m/%d/%Y").strftime('%Y-%m-%d')
                    source = content.find_element(By.CLASS_NAME,'information_source_value').text
                    op[2].click()
                    footnote = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_SelectionFootnotes').find_element(By.CLASS_NAME,'footnote_note_value ').text
                    table_filter = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0].find_element(By.TAG_NAME,'a')
                    table_filter.click()
                    arab_countries = ['Algeria', 'Bahrain', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan', 'Kuwait', 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Syrian AR' , 'Sudan' , 'Tunisia', 'United Arab Em', 'Yemen']
                    from selenium.webdriver.support.ui import Select
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    import csv
                    country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                    select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                    driver.execute_script("arguments[0].style.display = 'block';", select_element)
                    select_element.is_displayed()
                    select = Select(select_element)
                    option_list = select.options
                    for i in arab_countries:
                        country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                        select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                        driver.execute_script("arguments[0].style.display = 'block';", select_element)
                        select_element.is_displayed()
                        select = Select(select_element)
                        option_list = select.options
                        select.select_by_visible_text(i)
                        technology = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_EventButtons')
                        tech_all = technology.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        tech_all.click()
                        year = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_EventButtons')
                        all_year = year.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        all_year.click()
                        continu = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_ButtonViewTable')
                        continu.click()
                        page_source = driver.page_source               
                        soup = BeautifulSoup(page_source, 'html.parser')
                        table = soup.find(id='ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable')
                        rows = table.find_all('tr')
                        with open(f'{base_path}\\{i} - {title}.csv','w',newline = '') as f:
                            thewriter = csv.writer(f)
                            header = [f'{col_0}','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
                            thewriter.writerow(header)
                            for row in rows[2:]:                 
                                cells = row.find_all(['td','th'])
                                cells_text = [cell.get_text(strip=True) for cell in cells]
                                thewriter.writerow(cells_text)
                        try:
                            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{i} - {title}.csv"
                            BodyDict["JsonDetails"]["table"] = f'{i} - {title}'
                            BodyDict["JsonDetails"]["description"] = desc_note + ' ' + footnote
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["topic"] = topic
                            BodyDict["JsonDetails"]["last_modified"] =  last_update
                            BodyDict["JsonDetails"]["created"] =  created
                            BodyDict["JsonDetails"]["periodicity"] = periodicity
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
                        r_country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_EventButtons')
                        remove_country = r_country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_DeselectAllButton')
                        remove_country.click()
                    driver.back()
                    try:
                        element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                        WebDriverWait(driver, timeout).until(element_present)
                    except TimeoutException:
                        print("Timed out waiting for page to load element")
                    tags = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
                elif p in [4]:
                    list_pcg = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[2].find_elements(By.CLASS_NAME,'AspNet-TreeView-Leaf')
                    topic = list_pcg[p].text
                    installed_mw = list_pcg[p].find_element(By.TAG_NAME,'a').get_attribute('href')
                    title = list_pcg[p].find_element(By.TAG_NAME,'span').text.replace('/area','').replace('Region/','').replace('/',' and ')
                    col_0 = title.split(',')[0]
                    driver.get(installed_mw)
                    about_table = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
                    about_table.click()
                    op = driver.find_element(By.ID,'AboutTable').find_elements(By.TAG_NAME,'a')
                    op[0].click()
                    content = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_divInformation')
                    created = content.find_element(By.CLASS_NAME,'information_lastupdated_value').text
                    created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
                    desc_note = content.find_element(By.CLASS_NAME,'information_unit_value').text
                    try:
                        periodicity = content.find_element(By.CLASS_NAME,'information_updatefrequency_value').text
                    except:
                        pass
                    last_update = content.find_element(By.CLASS_NAME,'information_nextupdate_value').text
                    last_update = datetime.datetime.strptime(last_update, "%m/%d/%Y").strftime('%Y-%m-%d')
                    source = content.find_element(By.CLASS_NAME,'information_source_value').text
                    table_filter = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0].find_element(By.TAG_NAME,'a')
                    table_filter.click()
                    arab_countries = ['Algeria', 'Bahrain', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan', 'Kuwait', 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Sudan' , 'Syrian Arab Republic' , 'Tunisia', 'United Arab Emirates', 'Yemen']
                    from selenium.webdriver.support.ui import Select
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    import csv
                    country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                    select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                    driver.execute_script("arguments[0].style.display = 'block';", select_element)
                    select_element.is_displayed()
                    select = Select(select_element)
                    option_list = select.options
                    for i in arab_countries:
                        country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                        select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                        driver.execute_script("arguments[0].style.display = 'block';", select_element)
                        select_element.is_displayed()
                        select = Select(select_element)
                        option_list = select.options
                        select.select_by_visible_text(i)
                        technology = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_EventButtons')
                        tech_all = technology.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        tech_all.click()
                        year = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_EventButtons')
                        all_year = year.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_SelectAllButton')
                        all_year.click()
                        continu = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_ButtonViewTable')
                        continu.click()
                        page_source = driver.page_source               
                        soup = BeautifulSoup(page_source, 'html.parser')
                        table = soup.find(id='ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable')
                        rows = table.find_all('tr')
                        with open(f'{base_path}\\{i} - {title}.csv','w',newline = '') as f:
                            thewriter = csv.writer(f)
                            header = [f'{col_0}','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
                            thewriter.writerow(header)
                            for row in rows[2:]:                 
                                cells = row.find_all(['td','th'])
                                cells_text = [cell.get_text(strip=True) for cell in cells]
                                thewriter.writerow(cells_text)

                        try:
                            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{i} - {title}.csv"
                            BodyDict["JsonDetails"]["table"] = f'{i} - {title}'
                            BodyDict["JsonDetails"]["description"] = desc_note + ' ' + footnote
                            BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                            BodyDict["JsonDetails"]["topic"] = topic
                            BodyDict["JsonDetails"]["last_modified"] =  last_update
                            BodyDict["JsonDetails"]["created"] =  created
                            BodyDict["JsonDetails"]["periodicity"] = periodicity
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
                        r_country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_EventButtons')
                        remove_country = r_country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_DeselectAllButton')
                        remove_country.click()
                    driver.back()
            
            finance = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[0].find_element(By.CLASS_NAME,'AspNet-TreeView-Expand')
            tag = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[0].text
            finance.click()
            list_finance = driver.find_elements(By.CLASS_NAME,'AspNet-TreeView-Root')[0].find_element(By.CLASS_NAME,'AspNet-TreeView-Leaf')
            topic = list_finance.text
            public_flows = list_finance.find_element(By.TAG_NAME,'a').get_attribute('href')
            title = list_finance.find_element(By.TAG_NAME,'span').text.replace('/area','').replace('Region/','')
            col_0 = title.split(',')[0]
            driver.get(public_flows)
            about_table = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
            about_table.click()
            op = driver.find_element(By.ID,'AboutTable').find_elements(By.TAG_NAME,'a')
            op[0].click()
            content = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_divInformation')
            created = content.find_element(By.CLASS_NAME,'information_lastupdated_value').text
            created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
            desc_note = content.find_element(By.CLASS_NAME,'information_unit_value').text
            try:
                periodicity = content.find_element(By.CLASS_NAME,'information_updatefrequency_value').text
            except:
                pass
            last_update = content.find_element(By.CLASS_NAME,'information_nextupdate_value').text
            last_update = datetime.datetime.strptime(last_update, "%m/%d/%Y").strftime('%Y-%m-%d')
            source = content.find_element(By.CLASS_NAME,'information_source_value').text
            op[2].click()
            footnote = driver.find_element(By.ID,'AboutTable').find_element(By.ID,'ctl00_ContentPlaceHolderMain_SelectionFootnotes').find_element(By.CLASS_NAME,'footnote_note_value ').text
            table_filter = driver.find_element(By.ID,'PageElements').find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')[0].find_element(By.TAG_NAME,'a')
            table_filter.click()
            arab_countries = ['Algeria', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan' , 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Sudan' , 'Tunisia', 'United Arab Emirates', 'Yemen']
            from selenium.webdriver.support.ui import Select
            from selenium.webdriver.common.action_chains import ActionChains
            from selenium.webdriver.common.keys import Keys
            import csv
            country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
            select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
            driver.execute_script("arguments[0].style.display = 'block';", select_element) 
            select_element.is_displayed()
            select = Select(select_element)
            option_list = select.options
            for i in arab_countries:
                country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesSelectPanel')
                select_element = country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox')
                driver.execute_script("arguments[0].style.display = 'block';", select_element)
                select_element.is_displayed()
                select = Select(select_element)
                option_list = select.options
                select.select_by_visible_text(i)
                technology = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_EventButtons')
                tech_all = technology.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_SelectAllButton')
                tech_all.click()
                year = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_EventButtons')
                all_year = year.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_SelectAllButton')
                all_year.click()
                continu = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_ButtonViewTable')
                continu.click()
                page_source = driver.page_source               
                soup = BeautifulSoup(page_source, 'html.parser')
                table = soup.find(id='ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable')
                rows = table.find_all('tr')
                l = []
                for tr in rows[3:]:
                    cells = tr.find_all(['td','th'])
                    cells_text = [cell.get_text(strip=True) for cell in cells]
                    l.append(cells_text)
                df = pd.DataFrame(l,columns=["year", "B"])
                df_1 = df[1:22]
                df_1.columns = ["year",f"{df.iloc[0,0]}"]
                df_2 = df[23:44]
                df_2.columns = ["year",f"{df.iloc[22,0]}"]
                df_3 = df[45:66]
                df_3.columns = ["year",f"{df.iloc[44,0]}"]
                df_4 = df[67:88]
                df_4.columns = ["year",f"{df.iloc[66,0]}"]
                df_5 = df[89:110]
                df_5.columns = ["year",f"{df.iloc[88,0]}"]
                df_6 = df[111:132]
                df_6.columns = ["year",f"{df.iloc[110,0]}"]
                df_7 = df[133:154]
                df_7.columns = ["year",f"{df.iloc[132,0]}"]
                df_8 = df[155:176]
                df_8.columns = ["year",f"{df.iloc[154,0]}"]
                df_9 = df[177:198]
                df_9.columns = ["year",f"{df.iloc[176,0]}"]
                df_10 = df[199:220]
                df_10.columns = ["year",f"{df.iloc[198,0]}"]
                df_11 = df[221:242]
                df_11.columns = ["year",f"{df.iloc[220,0]}"]
                df_12 = df[243:264]
                df_12.columns = ["year",f"{df.iloc[242,0]}"]
                df_13 = df[265:286]
                df_13.columns = ["year",f"{df.iloc[264,0]}"]
                df_14 = df[287:308]
                df_14.columns = ["year",f"{df.iloc[286,0]}"]
                df_15 = df[309:330]
                df_15.columns = ["year",f"{df.iloc[308,0]}"]
                df_16 = df[331:352]
                df_16.columns = ["year",f"{df.iloc[330,0]}"]
                df_17 = df[353:374]
                df_17.columns = ["year",f"{df.iloc[352,0]}"]
                df_18 = df[375:396]
                df_18.columns = ["year",f"{df.iloc[374,0]}"]
                df_19 = df[397:418]
                df_19.columns = ["year",f"{df.iloc[396,0]}"]
                pdList = [df_1, df_2, df_3 , df_4 , df_5 , df_6 , df_7 , df_8 , df_9 , df_10 , df_11 , df_12 , df_13 , df_14 , df_15 , df_16 , df_17 , df_18 , df_19 ] 
                df = reduce(lambda  left,right: pd.merge(left,right,on=['year'],how='outer'), pdList)
                df.to_csv(f'{base_path}\\{i} - {title}.csv', index=False)

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/IRENA/{i} - {title}.csv"
                    BodyDict["JsonDetails"]["table"] = f'{i} - {title}'
                    BodyDict["JsonDetails"]["description"] = desc_note + ' ' + footnote
                    BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                    BodyDict["JsonDetails"]["topic"] = topic
                    BodyDict["JsonDetails"]["last_modified"] =  last_update
                    BodyDict["JsonDetails"]["created"] =  created
                    BodyDict["JsonDetails"]["periodicity"] = periodicity
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
                r_country = driver.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_EventButtons')
                remove_country = r_country.find_element(By.ID,'ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_DeselectAllButton')
                remove_country.click()
            driver.back()
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME,'m-SideMenu__link'))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load element")
            tags = driver.find_elements(By.CLASS_NAME,'m-SideMenu__link')
            break

execute() 
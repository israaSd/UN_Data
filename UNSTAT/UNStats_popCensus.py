from bs4 import BeautifulSoup
import requests
import logging
import pandas as pd
from unidecode import unidecode
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
import shutil
import time
import os
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
from Hashing.HashScrapedData import _hashing
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNStat_popcensus'


def execute():
    # format of the log message in specific file when download the data
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNStat_popcensus_out.log"),
                                logging.StreamHandler()], level=logging.INFO)

    response = requests.get('https://unstats.un.org/unsd/demographic-social/products/dyb/#censusdatasets')
    soup = BeautifulSoup(response.content, 'html.parser')

    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNStat_popcensus'
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    links_1=[]
    titles_1=[]
    parent_node=soup.find(class_='col-sm-12')
    child_node=parent_node.find_all(class_='list-unstyled margin-bottom-15')
    for each in child_node:
        tag = parent_node.find_all("h4")[child_node.index(each)].text
        datasets= each.find_all('li') #.find('a')['href']
        for i in datasets:
            titles = i.text.replace('- View data','')
            titles = titles.replace('\n','')
            titles = titles.replace(',',' ')
            titles = titles.replace('/',' ')
            titles_1.append(titles)
            links = i.find('a')['href']
            links_1.append(links)
            #for link in links_1[:2]:
            driver.get(links)
            #print(link)
            title = driver.find_element(By.CLASS_NAME,'SeriesMeta').find_element(By.TAG_NAME,'h2').text.replace('\n','').replace(',',' ').replace('/',' ').replace('Search glossaries','')
            driver.find_element(By.ID,'ctl00_main_actions_download').click()
            time.sleep(2)
            driver.find_element(By.ID,'downloadCommaLink').click()
            time.sleep(10)
            for zipfile in os.listdir(base_path):
                if zipfile.endswith(".zip"):
                    filename = zipfile.split(".zip")[0]
                    shutil.unpack_archive(f"{base_path}\\{zipfile}", base_path)
                    print(zipfile)
                    os.remove(f"{base_path}\\{zipfile}")
                    os.rename(f"{base_path}\\{filename}.csv", f"{base_path}\\{title}.csv")

            footnot = []
            df = pd.read_csv(f'{base_path}\\{title}.csv')
            ind = df.iloc[:,1].index[df.iloc[:,1]== 'Footnote'].to_list()
            footnotes = df.iloc[ind[0]:]
            footnotes = footnotes.iloc[:,0:2]
            for i in range(len(footnotes.index)):
                f = footnotes.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                footnot.append(f[0])
            footnot = [' , '.join(map(str,footnot))]
            df = df.iloc[:ind[0]]
            df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
            description = footnot[0]
            df.to_csv(f'{base_path}\\{title}.csv')
            

            try:
                BodyDict = {
                    "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNStat_popcensus/{title}.csv", #* Point to downloaded data for conversion #
                    "JsonDetails":{
                            ## Required
                            "organisation": "un-agencies",
                            "source": "UNSTATS",
                            "source_description" : "The United Nations Statistics Division collects from all the National Statistical Offices several population censuses' datasets. The data are collected via the Demographic Yearbook census questionnaires. The censuses' datasets reported by the National Statistical Offices for the censuses conducted worldwide during the period 1995 - Present are available below.",
                            "source_url" : "https://unstats.un.org/unsd/demographic-social/products/dyb/#censusdatasets",
                            "table" : title,
                            "description" : description,
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
                                "name": ""
                            }],
                            "limitations":"",
                            "concept":  "",
                            "periodicity":  "",
                            "topic":  tag,
                            "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                            "last_modified": "",                #* ""               ""                  ""              ""
                            "TriggerTalend" :  False,    #* initialise to True for production
                            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/unstat_popcensus" #* initialise as empty string for production.
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
import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import urllib.request
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
import shutil
import time
import os
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNDESA_part_1_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_1' # local, gets current working directory
    base_path
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    links_csv = []
    titles = []
    descriptions = []
    tags_name = []

    driver.get("https://www.un.org/development/desa/pd/data-landing-page")
    parent_node = driver.find_element(By.CLASS_NAME,'pane-content')
    topics = parent_node.find_element(By.TAG_NAME,'li')
    topic = topics.text
    link = topics.find_element(By.TAG_NAME,'a').get_attribute('href')
    driver.get(link)
    concept = driver.find_element(By.CLASS_NAME,'IntroductionText').text
    download_data_files = driver.find_element(By.CLASS_NAME,'TileLine')
    link_data_files = download_data_files.find_element(By.TAG_NAME,'a').get_attribute('href').replace("MostUsed","CSV")
    link_data_files = link_data_files + 'Standard/CSV/'
    driver.get(link_data_files)
    table =  driver.find_element(By.ID,'kgrData').find_element(By.TAG_NAME,'table')
    rows = table.find_elements(By.TAG_NAME,'tr')
    for row in rows[1:]:
        cells = row.find_elements(By.TAG_NAME,'td')
        cells_text = [cell.text for cell in cells]
        title = cells_text[3].split('\n')

        for cell in cells:
            div = cell.find_elements(By.CLASS_NAME,'FileList')
            for d in div:
                links = d.find_elements(By.TAG_NAME,'a')
                for link in links:
                    link_csv = link.get_attribute('href')
                    # print(link_csv)
                    links_csv.append(link_csv)
        for i in title:
            descr = cells_text[4].replace('\n','')
            descriptions.append(descr)
            tag = cells_text[2]
            tags_name.append(tag)
            title = cells_text[2] + ' - ' + i.partition('(')[0]
            if '\n' not in cells_text[4]:
                title = cells_text[2] + ' - ' + i.partition('(')[0] + ', ' + cells_text[4]
            titles.append(title.replace('.',''))
    for i in range(len(titles)):
        if links_csv[i].endswith('csv'):
            description = descriptions[i]
            #print(titles[i])
            #print(links_csv[i])
            urllib.request.urlretrieve(links_csv[i],f"{base_path}\\{titles[i]}.csv")
    
        elif links_csv[i].endswith('zip'):
            description = descriptions[i]
            title_file = titles[i]
            #print(links_csv[i])
            #print(description)
            tag = tags_name[i]
            print(tag)
            urllib.request.urlretrieve(links_csv[i],f"{base_path}\\{title_file.replace('.','')}.zip")
            from zipfile import ZipFile
            for zipfiles in os.listdir(base_path):
                if zipfiles.endswith(".zip"):
                    filename = zipfiles.split(".zip")[0]
                    shutil.unpack_archive(f"{base_path}\\{zipfiles}", base_path)
                    name_1 = ZipFile(f'{base_path}\\{zipfiles}').namelist()
                    name = name_1
                    os.remove(f"{base_path}\\{zipfiles}")
                    os.rename(f"{base_path}\\{name[0]}",f"{base_path}\\{filename}.csv")


            file_note = '//10.30.31.77/data_collection_dump/RawData/DESA_part_1/Notes - File , Location notes.csv'
            import unidecode
            import pandas as pd
            notes = []
            df = pd.read_csv(file_note)
            df.iloc[:,0] = df.iloc[:,1].str.encode('ascii', 'ignore').str.decode('ascii')
            for i in range(len(df.index)):
                f = df.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                notes.append(f[0])
            note = [' , '.join(map(str,notes))]
            note =  'Notes : Text'+ ' , ' + note[0]
            file_indc = '//10.30.31.77/data_collection_dump/RawData/DESA_part_1/Demographic Indicators - Indicator reference .csv'
            import pandas as pd
            indcs = []
            df = pd.read_csv(file_indc)
            for i in range(len(df.index)):
                f = df.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                indcs.append(f[0])
            indc = [' , '.join(map(str,indcs))]
            indc =  'IndicatorNo : Topic : Indicator : IndicatorName : Unit'+ ' , ' + indc[0]
            note_desc = indc + '\n' + note
            #print(title_file)
            #print(description + '\n\n' + note_desc)
            try:
                BodyDict = {
                    "JobPath":f"//10.30.31.77/data_collection_dump/RawData/DESA_part_1/{title_file}.csv", #* Point to downloaded data for conversion #
                    "JsonDetails":{
                            ## Required
                            "organisation": "un-agencies",
                            "source": "DESA",
                            "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
                            "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
                            "table" : title_file,
                            "description" : description + '\n\n' + note_desc, #+ indc + '\n' + note,
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
                            "concept":  concept,
                            "periodicity":  "",
                            "topic":  topic,
                            "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                            "last_modified": "",                #* ""               ""                  ""              ""
                            "TriggerTalend" :  False,    #* initialise to True for production
                            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/Desa_part_1" #* initialise as empty string for production.
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
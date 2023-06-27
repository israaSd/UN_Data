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
from bs4 import BeautifulSoup
import shutil
import time
import os
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI


def execute():
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/DESA_part_4/UNDESA_part_4_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = f"\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_4" # local, gets current working directory
    base_path
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    import timer
    link_tag = []
    base_path_test = ''
    driver.get("https://www.un.org/development/desa/pd/data-landing-page")
    parent_node = driver.find_element(By.CLASS_NAME,'pane-content')
    topics = parent_node.find_elements(By.TAG_NAME,'li')
    for t in topics:
        BodyDict = {
            "JobPath":"", #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "un-agencies",
                    "source": "DESA",
                    "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
                    "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
                    "table" : '',
                    "description" : '', #+ indc + '\n' + note,
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
                        "name": ""
                    }],
                    "limitations":"",
                    "concept":  '',
                    "periodicity":  "",
                    "topic":  '',
                    "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": "",                #* ""               ""                  ""              ""
                    "TriggerTalend" :  False,    #* initialise to True for production
                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_4" #* initialise as empty string for production.
                }
            }
        if t.text == 'World Marriage Data':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                # print(concept)
                driver.get(link)
                time.sleep(3)
                link_excel = driver.find_element(By.ID,'home').find_element(By.TAG_NAME,'a').get_attribute('href')
                print(link_excel)
                if link_excel.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link_excel, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                    import pandas as pd
                    info_notes = []
                    descriptions = []
                    file = f'{base_path}\\{title_f}.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df_info_note =  pd.read_excel(file,sheets[0])
                    info_note = df_info_note.iloc[4,1]
                    df_desc = pd.read_excel(file,sheets[1])
                    df_desc = df_desc.iloc[:,1:]
                    for i in range(len(df_desc.index)):
                        f = df_desc.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        descriptions.append(f[0])
                    description = [' ,\n '.join(map(str,descriptions))][0]
                    for sheet in sheets[2:]:
                        df_data = pd.read_excel(file,sheet)
                        title_marg = df_data.columns[0]
                        title_marg = title_marg.replace('  \n','').replace(' \n','').replace('\n','')
                        df_data.columns = df_data.iloc[1].to_list()
                        df_data = df_data[2:]
                        df_data.to_excel(f'{base_path}\\{title_marg}.xlsx',index=False)
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_4/{title_marg}.xlsx'
                            BodyDict["JsonDetails"]["table"] = title_marg
                            BodyDict["JsonDetails"]["description"] = description + '\n' + info_note
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] =  page_header
                            BodyDict["JsonDetails"]["tags"][0]["name"] =topic
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
                time.sleep(5)
                # driver.back()
            driver.back()
            driver.back()
            break

execute()
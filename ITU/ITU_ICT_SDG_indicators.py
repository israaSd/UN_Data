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
import urllib.request
from Hashing.HashScrapedData import _hashing
from bs4 import BeautifulSoup
import shutil
import time
import os
import pdfplumber
import logging

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/ITU_ICT_SDG_Indicators_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    from pyshadow.main import Shadow
    from py7zr import unpack_7zarchive
    from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\ITU ICT SDG indicators'

    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\ITU ICT SDG indicators" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://www.itu.int/en/ITU-D/Statistics/Pages/SDGs-ITU-ICT-indicators.aspx')
    time.sleep(3)

    datasets = driver.find_element(By.CLASS_NAME,'contentNew._invisibleIfEmpty')
    list_datasets = datasets.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
    for elem in list_datasets:
        dataset = elem.text.replace(':',',')
        indicator_report = elem.text.split(':')[0]
        download = elem.find_element(By.TAG_NAME,'a')
        download.click()
        time.sleep(1)
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
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        os.rename(file,f'{base_path}\\{dataset}.xlsx')

        notes = driver.find_elements(By.CLASS_NAME,'ms-rteTableEvenCol-3')
        for note in notes:
            if indicator_report in note.text:
                links = note.find_elements(By.TAG_NAME,'a')
                for link in links:
                    link_note = link.get_attribute('href')
                    if link_note.endswith('.pdf'):
                        if 'Metadata' in link_note:
                            pdf = link_note
                            pdf_name = pdf.split('files/')[1]
                            urllib.request.urlretrieve(pdf,f"{base_path}\\{pdf_name}")
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
                            conten=[]
                            with pdfplumber.open(f"{base_path}\\{pdf_name}") as pdf:
                                text = pdf.pages
                                for te in text:
                                    content = te.extract_text()
                                    conten.append(content)
                            content_all = ' '.join([str(elem) for elem in conten])
                            goal = content_all.split('0.a. Goal (SDG_GOAL) \n')[1].split('0.b. Target (SDG_TARGET) \n')[0].replace(' \n',' ')
                            target = content_all.split('0.b. Target (SDG_TARGET) \n')[1].split('0.c. Indicator (SDG_INDICATOR) \n')[0].replace(' \n',' ')
                            indicator = content_all.split('0.c. Indicator (SDG_INDICATOR) \n')[1].split('0.d. Series (SDG_SERIES_DESCR) \n')[0].replace(' \n',' ')
                            last_update = content_all.split('0.e. Metadata update (META_LAST_UPDATE) \n')[1].split('0.f. Related indicators (SDG_RELATED_INDICATORS) \n')[0].replace(' \n',' ')
                            try:
                                definition = content_all.split('Definition: \n')[1].split('Concepts: \n')[0].replace(' \n',' ')
                            except:
                                definition = content_all.split('Definitions: \n')[1].split('Concepts: \n')[0].replace(' \n',' ')
                            concept = content_all.split('Concepts: \n')[1].split('2.b. Unit of measure (UNIT_MEASURE) \n')[0].replace(' \n',' ').split('   Last updated: 2021-08-20 ')[0]
                            unit_measure = content_all.split('2.b.')[1].split('2.c.')[0].replace(' \n',' : ')
                            classification = content_all.split('2.c. Classifications (CLASS_SYSTEM) \n')[1].split('3. Data source type and data collection method \n')[0].replace(' \n',' ')
                            source = content_all.split('(SRC_TYPE_COLL_METHOD) \n')[1].split('4. Other methodological considerations (OTHER_METHOD) \n')[0].replace(' \n',' ')
                            coverage = content_all.split('5. Data availability and disaggregation (COVERAGE) \n')[1].split('6. Comparability / deviation from international standards \n')[0].replace(' \n',' ')
                            description = goal  + '. ' + unit_measure + '. ' + target + '. ' + indicator + '. ' + definition
                            description = description.replace('Last updated: 2021-08-20','')
                            limitaion = classification + '. ' + coverage
                            last_update = last_update.replace(' ','')
                            try:
                                BodyDict = {
                                "JobPath":f'//10.30.31.77/data_collection_dump/RawData/ITU ICT SDG indicators/{dataset}.xlsx', #* Point to downloaded data for conversion 
                                "JsonDetails":{
                                        ## Required
                                        "organisation": "third-parties",
                                        "source": "ITU",
                                        "source_description" : "As the UN specialized agency for ICTs, ITU is the official source for global ICT statistics.",
                                        "source_url" : "https://www.itu.int/en/ITU-D/Statistics/Pages/SDGs-ITU-ICT-indicators.aspx",
                                        "table" : dataset,
                                        "description" : description, 
                                        ## Optional
                                        "JobType": "JSON",
                                        "CleanPush": True,
                                        "Server": "str",
                                        "UseJsonFormatForSQL":  False,
                                        "CleanReplace":True,
                                        "MergeSchema": False,
                                        "tags": [{
                                            "name": 'ICT SDG indicators'
                                        }],
                                        "additional_data_sources": [{
                                            "name": source,
                                            "url" : ""
                                        }],
                                        "limitations":limitaion,
                                        "concept": concept,
                                        "periodicity":  "",
                                        "topic":  dataset,
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

execute() 
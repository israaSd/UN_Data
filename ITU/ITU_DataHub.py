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
from Hashing.HashScrapedData import _hashing
import time
import os
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
import logging

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/ITU_DataHub_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\ITU Data Hub'

    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\ITU Data Hub" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://datahub.itu.int/indicators/')

    from selenium.common.exceptions import TimeoutException
    content = driver.find_element(By.CLASS_NAME,'styles_root__kuX3t')
    indicators = content.find_element(By.TAG_NAME,'ul').find_elements(By.CLASS_NAME,'styles_item__4wcw_')
    for indicator in indicators:
        indicator_name = indicator.find_element(By.TAG_NAME,'h2').text
        list_indicators = indicator.find_element(By.CLASS_NAME,'styles_root__KNp6w').find_elements(By.CLASS_NAME,'styles_item__70VBH')
        for list_indicator in list_indicators:
            part_indicator = list_indicator.find_element(By.TAG_NAME,'h3').text.replace('/',' and ')
            list_parts_indicator = list_indicator.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
            for elem_part_indicator in list_parts_indicator:
                # dataset_name = elem_part_indicator.text
                # dataset_name = dataset_name.replace(':','').replace('/',',').replace('?','')#.replace('&','and')
                dataset = elem_part_indicator.find_element(By.TAG_NAME,'a').get_attribute('href')
                driver.get(dataset)
                time.sleep(2)
                timeout = 20
                try:
                    element_present = EC.presence_of_element_located((By.CLASS_NAME,'styles_header__N7Zbu'))
                    WebDriverWait(driver, timeout).until(element_present)
                except TimeoutException:
                    print("Timed out waiting for page to load DOWNLOAD")
                desc =  driver.find_element(By.CLASS_NAME,'styles_header__N7Zbu')
                desc = desc.text.replace('\n','  ')
                title = driver.find_element(By.CLASS_NAME,'styles_h1-alt__Ukj7x').text.replace(':','').replace('/',',').replace('?','')
                download = driver.find_element(By.CLASS_NAME,'styles_actions__uxvrQ').find_element(By.TAG_NAME,'a')
                download.click()
                if driver.find_element(By.TAG_NAME,'body').text == '{"message": "Endpoint request timed out"}' :
                    print('{"message": "Endpoint request timed out"}')
                    driver.back()
                    driver.back()
                    time.sleep(2)
                elif driver.find_element(By.TAG_NAME,'body').text == '(intermediate value) is not iterable' :
                    print('(intermediate value) is not iterable')
                    driver.back()
                    driver.back()
                    time.sleep(2)
                else:
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
                    filename = os.path.split(file)[1]
                    # filename = filename.split('_1')[0]
                    new_filename = f'{indicator_name} {part_indicator} {title}'
                    os.rename(file,f'{base_path}\\{new_filename}.csv')
                    driver.back()
                    time.sleep(2)
                    try:
                        BodyDict = {
                            "JobPath":f"//10.30.31.77/data_collection_dump/RawData/ITU Data Hub/{new_filename}.csv", #* Point to downloaded data for conversion 
                            "JsonDetails":{
                                    ## Required
                                    "organisation": "third-parties",
                                    "source": "ITU",
                                    "source_description" : "The International Telecommunication Union (ITU) is the United Nations specialized agency for information and communication technologies - ICTs.",
                                    "source_url" : "https://datahub.itu.int/data/",
                                    "table" : new_filename,
                                    "description" : desc,
                                    ## Optional
                                    "JobType": "JSON",
                                    "CleanPush": True,
                                    "Server": "str",
                                    "UseJsonFormatForSQL":  False,
                                    "CleanReplace":True,
                                    "MergeSchema": False,
                                    "tags": [{
                                        "name": part_indicator
                                    }],
                                    "additional_data_sources": [{
                                        "name": ''
                                    }],
                                    "limitations":'',
                                    "concept":  '',
                                    "periodicity":  '',
                                    "topic":  indicator_name,
                                    "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                    "last_modified": '',                #* ""               ""                  ""              ""
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
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNFCCC\\Time Series"
import logging
from selenium.webdriver.support.ui import Select
from Hashing.HashScrapedData import _hashing
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
import time
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

logging.basicConfig(handlers=[logging.FileHandler("\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs\\UNFCCC_TimeSeries_out.log"),
                        logging.StreamHandler()], level=logging.INFO)
chrome_options = webdriver.ChromeOptions()
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNFCCC\\Time Series" # local, gets current working directory
prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
chrome_options.add_experimental_option('prefs', prefs)
# chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.maximize_window()

driver.get('https://di.unfccc.int/time_series')
time.sleep(6)
PageContent = driver.find_element(By.CLASS_NAME,'PageContent')
theme = PageContent.find_element(By.TAG_NAME,'h1').text
tag = theme.split(' -')[0]
select_element = PageContent.find_element(By.CLASS_NAME,'party-data-filter')
select = Select(select_element)
option_list = select.options
for op in option_list:
    select.select_by_visible_text(op.text)
    time.sleep(3)
    topic = PageContent.find_element(By.TAG_NAME,'p').text.replace('/',', ').replace(':','')
    title = tag + ' ' + topic
    title = title.replace('₂','2').replace('₆','6').replace('₃','3').replace('₄','4')
    unit = topic.split(', in ')[1]
    table = driver.find_element(By.CLASS_NAME,'dataTable.no-footer')
    head = []
    thead = table.find_element(By.TAG_NAME,'thead').find_elements(By.TAG_NAME,'th')
    for th in thead:
        head.append(th.text)
    rows = table.find_elements(By.TAG_NAME,'tr')
    l = []
    for tr in rows[1:]:
        cells = tr.find_elements(By.TAG_NAME,'td')
        cells_text = [cell.text for cell in cells]
        l.append(cells_text)
    df = pd.DataFrame(l,columns=head) 
    unit = [unit] * len(df.index) 
    df.insert(1,'Unit',unit)
    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
    notes = driver.find_element(By.CLASS_NAME,'disclaimer-notes').text
    PageContent = driver.find_element(By.CLASS_NAME,'PageContent')
    theme = PageContent.find_element(By.TAG_NAME,'h1').text
    tag = theme.split(' -')[0]
    select_element = PageContent.find_element(By.CLASS_NAME,'party-data-filter')
    select = Select(select_element)
    option_list = select.options
    try:
        BodyDict = {
        "JobPath":f'//10.30.31.77/data_collection_dump/RawData/UNFCCC/Time Series/{title}.xlsx', #* Point to downloaded data for conversion #
        "JsonDetails":{
                ## Required
                "organisation": "un-agencies",
                "source": "UNFCCC",
                "source_description" : "The United Nations Framework Convention on Climate Change established an international environmental treaty to combat 'dangerous human interference with the climate system', in part by stabilizing greenhouse gas concentrations in the atmosphere.",
                "source_url" : "https://di.unfccc.int",
                "table" : title ,
                "description" : notes  ,
                ## Optional
                "JobType": "JSON",
                "CleanPush": True,
                "Server": "str",
                "UseJsonFormatForSQL":  False,
                "CleanReplace":True,
                "MergeSchema": False,
                "tags": [
                            {"name": tag}
                        ],
                "additional_data_sources": [{       
                        "name": '',        
                        "url": ''  ## this object will be ignored if "name" is empty    }
                }],
                "limitations":'',
                "concept":  '',
                "periodicity":  '',
                "topic": title ,
                "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified":'' ,                #* ""               ""                  ""              ""
                "TriggerTalend" :  False,    #* initialise to True for production
                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/UNFCCC/Time Series" #* initialise as empty string for production.
            }
        }
        # tablenom = BodyDict['JsonDetails']['table']

        # hashmessage = _hashing(BodyDict['JsonDetails']['source'], tablenom, BodyDict["JobPath"])

        # if hashmessage["Trigger_InferSchema"] == True and hashmessage["Success"] == True:

        TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
        TriggerInferShemaToJsonAPIClass.TriggerAPI()
        logging.info(f"Conversion successful - {title} ")
        print(BodyDict)
        #     logging.info(f"Conversion successful - {tablenom}, hashmessage: {hashmessage['message']}")

        # # logging.info(f"Conversion successful for {dataset}")
        # elif hashmessage['Success'] == True and hashmessage['Trigger_InferSchema'] == False:

        #     # dont trigger conversion nor talend
        #     logging.info(f"{hashmessage['message']}")

        # elif hashmessage['Success'] == False:
        #     logging.info(f"Hashing error or Unexpected Issue: {hashmessage['message']}")
    except  Exception as err:
        print(err)
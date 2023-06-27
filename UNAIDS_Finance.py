base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNAIDS_finance"
import logging
from selenium.webdriver.support.ui import Select
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
import time
import os
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs\\finance_UNAIDS.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNAIDS_finance" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://hivfinancial.unaids.org/hivfinancialdashboards.html')
    time.sleep(2)


    country = driver.find_element(By.ID,'countryAHref')
    country.click()
    time.sleep(1)
    select_element = driver.find_element(By.ID,'lstCountries')
    select = Select(select_element)
    option_list = select.options
    for i in option_list[94:95]:
        c = i.text
        select.select_by_visible_text(i.text)
        time.sleep(2)
        content = driver.find_element(By.CLASS_NAME,'ui.five.column.relaxed.equal.height.divided.stackable.grid')
        list_links = content.find_elements(By.CLASS_NAME,'ui.link.list')
        for l in list_links[6:]:
            ind = list_links.index(l)
            top = content.find_elements(By.TAG_NAME,'h4')[ind].text
            items = l.find_elements(By.CLASS_NAME,'item')
            for item in items:
                topic = item.text
                item.click()
                time.sleep(5)
                iframe = driver.find_element(By.ID,'frameIt')
                driver.switch_to.frame(iframe)
                boxes = driver.find_elements(By.CLASS_NAME,'widget-container.dashboard-item-container')
                for box in boxes:
                    j = boxes.index(box)
                    if len(box.find_elements(By.CLASS_NAME, 'title.variable-left-margin.with-description'))>0 or len(box.find_elements(By.CLASS_NAME, 'title.variable-left-margin'))>0:
                        try :
                            m = box.find_element(By.CLASS_NAME, 'title.variable-left-margin.with-description')
                        except :
                            try :
                                m = box.find_element(By.CLASS_NAME, 'title.variable-left-margin')
                            except:
                                pass
                        js_code = "arguments[0].scrollIntoView();"
                        driver.execute_script(js_code, m)
                        if  box.find_element(By.CLASS_NAME,'error-message').text == 'Query returned no matching rows.' or  box.find_element(By.CLASS_NAME,'alert-message').text =='Negative numbers are not supported in proportional charts.':
                            pass
                        else:
                            title = m.text
                            if title == '':
                                pass
                            elif f'{c} {title}.csv' in os.listdir(base_path):
                                pass
                            else:
                                m.click()
                                time.sleep(1)
                                more_options = box.find_element(By.NAME,'expand')
                                more_options.click()
                                time.sleep(2)
                                download = driver.find_element(By.XPATH,"//*[contains(text(), 'Download Data')]")
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
                                    time.sleep(3) 
                                    newest_file = latest_download_file()
                                    if "crdownload" in newest_file:
                                        fileends = "crdownload"
                                        # time.sleep(5)
                                    else:
                                        fileends = "None"
                                latest_download_file() 
                                file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
                                title = title.replace('/',' ').replace(':',' ').replace('"',' ').replace('*',' ')
                                try:
                                    os.rename(file,f'{base_path}\\{c} {title}.csv')
                                    try:
                                        source = driver.find_elements(By.XPATH,"//*[contains(text(), 'Data source')]")[j+1].text
                                    except:
                                        try:
                                            source = driver.find_elements(By.XPATH,"//*[contains(text(), 'Data Source')]")[j+1].text
                                        except:
                                            source = ''
                                    try:
                                        BodyDict = { 
                                            "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNAIDS_finance/{c} {title}.csv", #* Point to downloaded data for conversion 
                                            "JsonDetails":{
                                                    ## Required
                                                    "organisation": "un-agencies",
                                                    "source": "UNAIDS",
                                                    "source_description" : "The Joint United Nations Programme on HIV and AIDS is the main advocate for accelerated, comprehensive and coordinated global action on the HIV/AIDS pandemic.",
                                                    "source_url" : "https://unaids.org/en",
                                                    "table" : f'{c} {title}',
                                                    "description" : f'country: {c} , {top} {topic} - {title}',
                                                    ## Optional
                                                    "JobType": "JSON",
                                                    "CleanPush": True,
                                                    "Server": "str",
                                                    "UseJsonFormatForSQL":  False,
                                                    "CleanReplace":True,
                                                    "MergeSchema": False,
                                                    "tags": [{
                                                        "name": 'Finance'
                                                    }],
                                                    "additional_data_sources": [{
                                                        "name": source
                                                    }],
                                                    "limitations":'',
                                                    "concept":  '',
                                                    "periodicity":  '',
                                                    "topic":  topic,
                                                    "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                                    "last_modified": '',                #* ""               ""                  ""              ""
                                                    "TriggerTalend" :  False,    #* initialise to True for production
                                                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/UNAIDS_finance" #* initialise as empty string for production.
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
                                except  Exception as err:
                                    print(err)
                driver.switch_to.default_content()
                country = driver.find_element(By.ID,'countryAHref')
                country.click()
                time.sleep(2)
                items = l.find_elements(By.CLASS_NAME,'item')
            content = driver.find_element(By.CLASS_NAME,'ui.five.column.relaxed.equal.height.divided.stackable.grid')
            list_links = content.find_elements(By.CLASS_NAME,'ui.link.list')
execute() 
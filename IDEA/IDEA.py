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
from selenium.common.exceptions import TimeoutException
from Hashing.HashScrapedData import _hashing
from bs4 import BeautifulSoup
import shutil
import time
import os
import pandas as pd
import logging
import pandas as pd
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\IDEA'

def execute():

    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\IDEA' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('ignore-certificate-errors')
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://www.idea.int/data-tools/data') 
    time.sleep(4)

    driver.find_element(By.ID,'popup-buttons').find_element(By.CLASS_NAME,'btn.agree-button.eu-cookie-compliance-default-button').click()

    description_metadata = []
    source_metadata = []
    databases = driver.find_elements(By.CLASS_NAME,'views-field.views-field-title')
    for database in databases:
        if database in databases[4:5]:
            database.find_element(By.TAG_NAME,'a').click()
            time.sleep(2)
            concept = driver.find_element(By.CLASS_NAME,'intro').text
            menu = driver.find_element(By.ID,'block-block-79')
            about = menu.find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
            about.click()
            time.sleep(4)
            description = driver.find_element(By.CLASS_NAME,'intro').text
            partners = driver.find_element(By.ID,'partnersSection').find_element(By.TAG_NAME,'p').find_elements(By.TAG_NAME,'a')
            name_1 = partners[1].text
            source_url_1 = partners[1].get_attribute('href')
            name_2 = partners[0].text
            source_url_2 = partners[0].get_attribute('href')
            source = [[name_1,source_url_1],[name_2,source_url_2]]
            description_metadata.append(description)
            source_metadata.append(source)
            driver.back()
            time.sleep(1)
            driver.back()
            time.sleep(1)
            databases = driver.find_elements(By.CLASS_NAME,'views-field.views-field-title')
        elif database in databases[6:7]:
            database.find_element(By.TAG_NAME,'a').click()
            time.sleep(2)
            content = driver.find_element(By.CLASS_NAME,'content')
            about = content.find_element(By.CLASS_NAME,'panel-group')
            overview = about.find_element(By.CLASS_NAME,'panel.panel-default').find_element(By.CLASS_NAME,'toc-filter-processed.collapsed')
            overview.click()
            time.sleep(1)
            description = about.find_element(By.CLASS_NAME,'panel.panel-default').text
            methodology = about.find_elements(By.CLASS_NAME,'panel.panel-default')[2].find_element(By.TAG_NAME,'a')
            methodology.click()
            time.sleep(1)
            source_1 = about.find_element(By.ID,'collapseTwo').find_elements(By.TAG_NAME,'p')[6].text
            source_2 = about.find_element(By.ID,'collapseTwo').find_elements(By.TAG_NAME,'p')[7].text
            name_1 = source_1 + ' ' + source_2
            source_url_1 = ''
            name_2 = ''
            source_url_2 = ''
            source = [[name_1,source_url_1],[name_2,source_url_2]]
            description_metadata.append(description)
            source_metadata.append(source)
            time.sleep(1)
            driver.back()
            time.sleep(2)
        else:
            database.find_element(By.TAG_NAME,'a').click()
            time.sleep(2)
            title = driver.find_element(By.TAG_NAME,'h1').text
            try:
                descriptions = driver.find_element(By.ID,'aboutText').find_elements(By.TAG_NAME,'p')
                try:
                    description = descriptions[0].text + descriptions[1].text + descriptions[2].text
                except:
                    description = descriptions[0].text + descriptions[1].text
            except:
                description = driver.find_element(By.ID,'node-269497').text
            time.sleep(1)
            driver.back()
            time.sleep(2)
            databases = driver.find_elements(By.CLASS_NAME,'views-field.views-field-title')
            name_1 = ''
            source_url_1 = ''
            name_2 = ''
            source_url_2 = ''
            source = [[name_1,source_url_1],[name_2,source_url_2]]
            description_metadata.append(description)
            source_metadata.append(source)

    description_metadata = [description_metadata[7],description_metadata[4],description_metadata[6],description_metadata[3],description_metadata[2],description_metadata[0],description_metadata[1],description_metadata[-1],description_metadata[5]]
    source_metadata = [source_metadata[7],source_metadata[4],source_metadata[6],source_metadata[3],source_metadata[2],source_metadata[0],source_metadata[1],source_metadata[-1],source_metadata[5]]

    driver.get('https://www.idea.int/advanced-search') 
    time.sleep(2)

    from selenium.webdriver.support.ui import WebDriverWait
    themes = driver.find_elements(By.CLASS_NAME,'theme.branch.collapsed')
    for j in range(len(themes)):
        questions = driver.find_element(By.ID,'tree-questions')
        theme = questions.find_elements(By.CLASS_NAME,'theme.branch.collapsed')[j]
        td =  theme.find_element(By.TAG_NAME,'td')
        label = td.find_element(By.TAG_NAME,'label')
        title = label.text
        label.find_element(By.TAG_NAME,'input').click()
        time.sleep(1)
        continents = driver.find_elements(By.CLASS_NAME,'continent.branch.collapsed')
        for continent in continents:
            label_country = continent.find_element(By.TAG_NAME,'label')
            check_box_country = label_country.find_element(By.TAG_NAME,'input')
            check_box_country.click()
            time.sleep(1)
            continents = driver.find_elements(By.CLASS_NAME,'continent.branch.collapsed')
        from selenium.webdriver.support.ui import Select
        years = driver.find_element(By.ID,'collapse3')
        select_element = years.find_element(By.ID,'time-range-from')
        select = Select(select_element)
        option_list = select.options
        first = option_list[1].text
        select.select_by_visible_text(first)  
        select_element = years.find_element(By.ID,'time-range-to')
        select = Select(select_element)
        option_list = select.options
        last = option_list[-1].text
        select.select_by_visible_text(last)   
        time.sleep(1)
        search = driver.find_element(By.ID,'submit-assisted-search')
        search.click()
        time.sleep(2)
        questions_reset = driver.find_element(By.CLASS_NAME,'tree.tree-container.tree-questions-container').find_element(By.ID,'question-reset')
        questions_reset.click()
        time.sleep(1)
        continent_reset = driver.find_element(By.CLASS_NAME,'tree.tree-container.tree-countries-container').find_element(By.ID,'countries-reset')
        continent_reset.click()
        time.sleep(1)
        time.sleep(30)
        delay = 40  # seconds
        try:
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME,'search-results')))
        except TimeoutException:
            print("Loading table took too much time!")
        data = driver.find_element(By.CLASS_NAME,'search-results').get_attribute("outerHTML")
        df  = pd.read_html(data)[0]
        columns = []
        for i in range(len(df.columns)):
            # col_1 = df.columns[i][0]
            col_2 = df.columns[i][1]
            col = col_2
            columns.append(col)
        df.columns = columns
        sum = df.index[df.iloc[:,0] == 'Zimbabwe'].tolist()[-1] + 1
        df = df.iloc[:sum,:]
        df.to_csv(f'{base_path}\\{title}.csv',index=False)
        time.sleep(3)
        from selenium.webdriver.common.action_chains import ActionChains
        element = driver.find_element(By.CLASS_NAME,"titlebar")
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        description = description_metadata[j]
        source_name_1 = source_metadata[j][0][0]
        source_url_1 = source_metadata[j][0][1]
        source_name_2 = source_metadata[j][1][0]
        source_url_2 = source_metadata[j][1][1]
        try:
            BodyDict = {
            "JobPath":f'//10.30.31.77/data_collection_dump/RawData/IDEA/{title}.csv', #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "third-parties",
                    "source": "IDEA",
                    "source_description" : "The International Institute for Democracy and Electoral Assistance (International IDEA) is an intergovernmental organization that supports sustainable democracy worldwide.",
                    "source_url" : "https://www.idea.int/data-tools/data",
                    "table" : title,
                    "description" : description,
                    ## Optional
                    "JobType": "JSON",
                    "CleanPush": True,
                    "Server": "str",
                    "UseJsonFormatForSQL":  False,
                    "CleanReplace":True,
                    "MergeSchema": False,
                    "tags": [
                                {"name":'unclassified' 
                                }
                            ],
                    "additional_data_sources": [{       
                            "name": source_name_1,        
                            "url": source_url_1  ## this object will be ignored if "name" is empty    }
                    },
                    {       
                            "name": source_name_2,        
                            "url": source_url_2  ## this object will be ignored if "name" is empty    }
                    }],
                    "limitations":'',
                    "concept":  '',
                    "periodicity":  '',
                    "topic": title ,
                    "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified":'' ,                #* ""               ""                  ""              ""
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
        themes = driver.find_elements(By.CLASS_NAME,'theme.branch.collapsed')

execute()
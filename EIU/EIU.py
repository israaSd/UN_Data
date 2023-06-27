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
from bs4 import BeautifulSoup
from Hashing.HashScrapedData import _hashing

import logging
import time
import os
import pandas as pd
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\EIU'

def execute() :
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/EIU_out.log"),
                                        logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\EIU" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://viewpoint.eiu.com/data')
    time.sleep(4)

    iframe = driver.find_element(By.ID,'sp_message_iframe_802788')
    driver.switch_to.frame(iframe)

    driver.find_element(By.CLASS_NAME,'message-component.message-button.no-children.focusable.teg-button.start-focus.sp_choice_type_11').click()

    browse_data = driver.find_element(By.CLASS_NAME,'ip_selections-wrapper')
    list_Series = browse_data.find_element(By.CLASS_NAME,'ip-tree-wrapper-column.ip_series-data-tree-container').find_element(By.CLASS_NAME,'root')
    series = list_Series.find_element(By.CLASS_NAME,'infinite-scroll-component').find_elements(By.CLASS_NAME,'node.tree')

    for j in range(len(series)):
        # print(serie.text)
        tag = series[j].text
        input = series[j].find_element(By.CLASS_NAME,'checkbox-item')
        input.click()
        time.sleep(1)
        geographies = browse_data.find_element(By.CLASS_NAME,'ip-tree-wrapper-column.ip_geographies-data-tree-container').find_element(By.CLASS_NAME,'ip_data-tree-container').find_element(By.CLASS_NAME,'header').find_element(By.CLASS_NAME,'ip_select-all-button')
        geographies.click()
        time.sleep(1)
        selection = browse_data.find_element(By.CLASS_NAME,'ip_selections-data-tree-container').find_element(By.CLASS_NAME,'ip_data-tree-wrapper.ip_data-tree-selection').find_element(By.CLASS_NAME,'ip_selections-buttons')
        view_table = selection.find_element(By.CLASS_NAME,'ds-button')
        view_table.click()
        time.sleep(5)
        delay = 8 # seconds
        try:
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME,'ipd_data-results_series-geo-selector')))
        except TimeoutException:
            print("Loading took too much time!")
        from selenium.webdriver.common.action_chains import ActionChains
        element = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector')
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        selector_data = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector').find_element(By.CLASS_NAME,'ipd_viewby-series-dropdown')
        part_Series = selector_data.find_element(By.CLASS_NAME,'select-by-series__value-container.select-by-series__value-container--has-value.css-1hwfws3')
        part_Series.click()
        time.sleep(3)
        categories = driver.find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')#.get_attribute('innerHTML')

        for k in range(len(categories)) :
            title = categories[k].text
            if title == '':
                time.sleep(2)
                delay = 17 # seconds
                try:
                    myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME,'ip_download')))
                except TimeoutException:
                    print("Loading took too much time!")
                try:
                    time.sleep(3)
                    no_results = driver.find_element(By.CLASS_NAME,'ipd_search-no-results')    
                    selector_data = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector').find_element(By.CLASS_NAME,'ipd_viewby-series-dropdown')
                    part_Series = selector_data.find_element(By.CLASS_NAME,'select-by-series__value-container.select-by-series__value-container--has-value.css-1hwfws3')
                    part_Series.click()
                    time.sleep(3)
                    categories = driver.find_element(By.CLASS_NAME,'select-by-series__menu.css-8plzl5-menu').find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')
                except:
                    # driver.find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')[k].click()
                    download = driver.find_element(By.CLASS_NAME,'ip_download').find_element(By.CLASS_NAME,'ds-actioned-link.ds-actioned-link--download')
                    download.click()
                    time.sleep(1)
                    csv_form = driver.find_element(By.CLASS_NAME,'ds__link-list').find_element(By.CLASS_NAME,'ds-navigation-link')
                    csv_form.click()
                    time.sleep(3)
                    def latest_download_file():
                        path = base_path
                        os.chdir(path)
                        files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                        newest = files[-1]

                        return newest

                    fileends = "crdownload"
                    while "crdownload" == fileends:
                        time.sleep(4) 
                        newest_file = latest_download_file()
                        if "crdownload" in newest_file:
                            fileends = "crdownload"
                            # time.sleep(5)
                        else:
                            fileends = "None"
                    latest_download_file() 
                    element = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col')
                    actions = ActionChains(driver)
                    actions.move_to_element(element).perform()
                    title = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col').find_element(By.TAG_NAME,'h2').text.split('\n')[1]
                    try:
                        driver.find_element(By.CLASS_NAME,'expandCollapseController').find_element(By.TAG_NAME,'a').click()
                    except:
                        pass
                    try:
                        reads_desc = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col').find_elements(By.CLASS_NAME,'ip_read-more-read-less.ds-navigation-link')
                        for d in reads_desc:
                            d.click()
                    except:
                        pass
                    descriptions = []
                    table_content = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col').find_element(By.TAG_NAME,'table')#.find_element(By.CLASS_NAME,'ipd_table-container.ip_container')
                    rows = table_content.find_elements(By.TAG_NAME,'tr')
                    for row in rows[1:]:
                        cells = row.find_elements(By.TAG_NAME,'td')
                        cells_text = [cell.text for cell in cells]
                        text = 'Country : ' + cells_text[1] + ', Unit : ' + cells_text[2] + ', Description : ' + cells_text[3] + ', Data specific notes : ' + cells_text[4] + ', Source : ' + cells_text[5] + ', Published : ' + cells_text[6]
                        descriptions.append(text)
                    description = [' \n\n '.join(map(str,descriptions))][0]  
                    element = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector')
                    actions = ActionChains(driver)
                    actions.move_to_element(element).perform()
                    file = '\\\\10.30.31.77\\data_collection_dump\\RawData\\EIU\\table.csv'
                    title = title.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ').replace('>',' ')
                    file_name = tag + ' ' + title
                    os.rename(file,f'{base_path}\\{file_name}.csv')
                    try:
                        BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/EIU/{file_name}.csv", #* Point to downloaded data for conversion #
                        "JsonDetails":{
                                ## Required
                                "organisation": "third-parties",
                                "source": "EIU",
                                "source_description" : "The Economist Intelligence Unit is the research and analysis division of the Economist Group, providing forecasting and advisory services through research and analysis, such as monthly country reports, five-year country economic forecasts, country risk service reports, and industry reports",
                                "source_url" : "https://viewpoint.eiu.com/data",
                                "table" : file_name ,
                                "description" : description  ,
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
                                "topic": file_name ,
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
                    selector_data = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector').find_element(By.CLASS_NAME,'ipd_viewby-series-dropdown')
                    part_Series = selector_data.find_element(By.CLASS_NAME,'select-by-series__value-container.select-by-series__value-container--has-value.css-1hwfws3')
                    part_Series.click()
                    time.sleep(2)
                    categories = driver.find_element(By.CLASS_NAME,'select-by-series__menu.css-8plzl5-menu').find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')

            else:
                title = title.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ').replace('>',' ')
                driver.find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')[k].click()
                time.sleep(6)
                from selenium.common.exceptions import TimeoutException
                delay = 17 # seconds
                try:
                    myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME,'ip_download')))
                except TimeoutException:
                    print("Loading took too much time!")
                
                try:
                    download = driver.find_element(By.CLASS_NAME,'ip_download').find_element(By.CLASS_NAME,'ds-actioned-link.ds-actioned-link--download')
                    download.click()
                    time.sleep(1)
                    # table_data = driver.find_element(By.ID,'Access-To-Local-Markets-0-4--download-options')
                    csv_form = driver.find_element(By.CLASS_NAME,'ds__link-list').find_element(By.CLASS_NAME,'ds-navigation-link')
                    csv_form.click()
                    time.sleep(3)
                    def latest_download_file():
                        path = base_path
                        os.chdir(path)
                        files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                        newest = files[-1]

                        return newest

                    fileends = "crdownload"
                    while "crdownload" == fileends:
                        time.sleep(4) 
                        newest_file = latest_download_file()
                        if "crdownload" in newest_file:
                            fileends = "crdownload"
                            # time.sleep(5)
                        else:
                            fileends = "None"
                    latest_download_file() 
                    file = '\\\\10.30.31.77\\data_collection_dump\\RawData\\EIU\\table.csv'
                    file_name = tag + ' ' + title
                    os.rename(file,f'{base_path}\\{file_name}.csv')
                    element = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col')
                    actions = ActionChains(driver)
                    actions.move_to_element(element).perform()
                    try:
                        driver.find_element(By.CLASS_NAME,'expandCollapseController').find_element(By.TAG_NAME,'a').click()
                    except :
                        pass
                    try:
                        reads_desc = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col').find_elements(By.CLASS_NAME,'ip_read-more-read-less.ds-navigation-link')
                        for d in reads_desc:
                            d.click()
                    except:
                        pass
                    descriptions = []
                    table_content = driver.find_element(By.CLASS_NAME,'ip_metadata-table.ip_metadata-table-with-link-col').find_element(By.TAG_NAME,'table')#.find_element(By.CLASS_NAME,'ipd_table-container.ip_container')
                    rows = table_content.find_elements(By.TAG_NAME,'tr')
                    for row in rows[1:]:
                        cells = row.find_elements(By.TAG_NAME,'td')
                        cells_text = [cell.text for cell in cells]
                        text = 'Country : ' + cells_text[1] + ', Unit : ' + cells_text[2] + ', Description : ' + cells_text[3] + ', Data specific notes : ' + cells_text[4] + ', Source : ' + cells_text[5] + ', Published : ' + cells_text[6]
                        descriptions.append(text)
                    description = [' \n\n '.join(map(str,descriptions))][0]  
                    try:
                        BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/EIU/{file_name}.csv", #* Point to downloaded data for conversion #
                        "JsonDetails":{
                                ## Required
                                "organisation": "third-parties",
                                "source": "EIU",
                                "source_description" : "The Economist Intelligence Unit is the research and analysis division of the Economist Group, providing forecasting and advisory services through research and analysis, such as monthly country reports, five-year country economic forecasts, country risk service reports, and industry reports",
                                "source_url" : "https://viewpoint.eiu.com/data",
                                "table" : file_name ,
                                "description" : description  ,
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
                                "topic": file_name ,
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
                    selector_data = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector').find_element(By.CLASS_NAME,'ipd_viewby-series-dropdown')
                    part_Series = selector_data.find_element(By.CLASS_NAME,'select-by-series__value-container.select-by-series__value-container--has-value.css-1hwfws3')
                    part_Series.click()
                    time.sleep(2)
                    categories = driver.find_element(By.CLASS_NAME,'select-by-series__menu.css-8plzl5-menu').find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')
                except:
                    time.sleep(3)
                    no_results = driver.find_element(By.CLASS_NAME,'ipd_search-no-results')
                    # driver.find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')[k].click()
                    # time.sleep(5)
                    selector_data = driver.find_element(By.CLASS_NAME,'ipd_data-results_series-geo-selector').find_element(By.CLASS_NAME,'ipd_viewby-series-dropdown')
                    part_Series = selector_data.find_element(By.CLASS_NAME,'select-by-series__value-container.select-by-series__value-container--has-value.css-1hwfws3')
                    part_Series.click()
                    time.sleep(3)
                    categories = driver.find_element(By.CLASS_NAME,'select-by-series__menu.css-8plzl5-menu').find_elements(By.CLASS_NAME,'select-by-series__option.css-1004i92-option')

        start_new = driver.find_element(By.CLASS_NAME,'ipd_search-container').find_element(By.CLASS_NAME,'ds-link-with-arrow-icon')
        start_new.click()
        time.sleep(4)
        browse_data = driver.find_element(By.CLASS_NAME,'ip_selections-wrapper')
        series = browse_data.find_element(By.CLASS_NAME,'ip-tree-wrapper-column.ip_series-data-tree-container').find_element(By.CLASS_NAME,'root').find_element(By.CLASS_NAME,'infinite-scroll-component').find_elements(By.CLASS_NAME,'node.tree')

execute()
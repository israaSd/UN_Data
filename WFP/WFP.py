from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\WFP"
import urllib.request
import logging
import pandas as pd
from selenium.webdriver.support.ui import Select
from re import search
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
import time
import os
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs\\WFP\\wfp_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\WFP" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    BodyDict = {
        "JobPath":"", #* Point to downloaded data for conversion 
        "JsonDetails":{
                ## Required
                "organisation": "un-agencies",
                "source": "WFP",
                "source_description" : "The World Food Programme is an international organization within the United Nations that provides food assistance worldwide. It is the world's largest humanitarian organization and the leading provider of school meals. Founded in 1961, WFP is headquartered in Rome and has offices in 80 countries.",
                "source_url" : "https://dataviz.vam.wfp.org/",
                "table" : "",
                "description" : "", 
                ## Optional
                "JobType": "JSON",
                "CleanPush": True,
                "Server": "str",
                "UseJsonFormatForSQL":  False,
                "CleanReplace":True,
                "MergeSchema": False,
                "tags": [{
                    "name": ""
                }],
                "additional_data_sources": [{
                    "name": "",
                    "url" : ""
                }],
                "limitations":"",
                "concept":  "",
                "periodicity":  "",
                "topic":  "",
                "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified": "",                #* ""               ""                  ""              ""
                "TriggerTalend" :  False,    #* initialise to True for production
                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/WFP" #* initialise as empty string for production.
            }
        }

    driver.get('https://dataviz.vam.wfp.org/economic_explorer/prices')
    countries = ['Egypt' , 'Iraq' , 'Jordan' , 'Lebanon' , 'Libya' , 'Sudan' , 'Yemen' , 'Algeria' , 'Mauritania' , 'Somalia']
    from selenium.webdriver.support.ui import Select
    parent_node = driver.find_element(By.CLASS_NAME,'content-wrapper')
    select_element = parent_node.find_element(By.CLASS_NAME,'selectpicker')
    select = Select(select_element)
    option_list = select.options
    for i in countries:
        select.select_by_visible_text(i)
        time.sleep(5)
        displayed = driver.find_element(By.CLASS_NAME,'wrapper')
        content = parent_node.find_element(By.CLASS_NAME,'content')
        flex = content.find_element(By.CLASS_NAME,'row')
        flex.click()
        time.sleep(5)
        header = flex.find_element(By.CLASS_NAME,'box-header')
        # tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
        download = header.find_element(By.ID,'btnDownloadData')
        download.click()
        time.sleep(5)
        Full_country_data = driver.find_element(By.ID,'btnFullDataDownload')
        Full_country_data.click()
        time.sleep(3)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        from datetime import date
        import datetime
        today = str(date.today())
        r_date = datetime.datetime.strptime(today, "%Y-%m-%d").strftime('_%Y%b%d')
        new_filename = filename.replace(f'{r_date}','')
        os.rename(file,f'{base_path}\\{new_filename}')
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{new_filename}'
            BodyDict["JsonDetails"]["table"] = new_filename.split('.')[0] 
            BodyDict["JsonDetails"]["description"] = new_filename.split('.')[0]
            BodyDict["JsonDetails"]["topic"] =  'Prices'
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'

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
    select.select_by_visible_text('Syrian Arab Republic')
    displayed = driver.find_element(By.CLASS_NAME,'wrapper')
    content = parent_node.find_element(By.CLASS_NAME,'content')
    flex = content.find_element(By.CLASS_NAME,'row')
    flex.click()
    time.sleep(50)
    header = flex.find_element(By.CLASS_NAME,'box-header')
    tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
    download = header.find_element(By.ID,'btnDownloadData')
    download.click()
    Full_country_data = driver.find_element(By.ID,'btnFullDataDownload')
    time.sleep(2)
    Full_country_data.click()
    time.sleep(3)
    file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    filename = os.path.split(file)[1]
    from datetime import date
    import datetime
    today = str(date.today())
    r_date = datetime.datetime.strptime(today, "%Y-%m-%d").strftime('_%Y%b%d')
    new_filename = filename.replace(f'{r_date}','')
    os.rename(file,f'{base_path}\\{new_filename}')
    try :
        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{new_filename}'
        BodyDict["JsonDetails"]["table"] = new_filename.split('.')[0]
        BodyDict["JsonDetails"]["description"] = new_filename.split('.')[0]
        BodyDict["JsonDetails"]["topic"] =  'Prices'
        BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'

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


    driver.get('https://dataviz.vam.wfp.org/economic_explorer/prices')
    form = driver.find_element(By.ID,'form1')
    form.click()
    aside= form.find_element(By.CLASS_NAME,'main-sidebar.sidebar-dark-primary.elevation-4')
    aside.click()
    content_aside = aside.find_element(By.CLASS_NAME,'sidebar')
    ul = content_aside.find_element(By.TAG_NAME,'ul')
    all_li = ul.find_elements(By.TAG_NAME,'li')
    li = all_li[3]
    a = li.find_element(By.TAG_NAME,'a')
    a.click()
    ul_2 = li.find_element(By.CLASS_NAME,'nav.nav-treeview')
    all_li_2 = ul_2.find_elements(By.CLASS_NAME,'nav-item')
    a_infl = all_li_2[1].find_element(By.TAG_NAME,'a')
    link_infl = a_infl.get_attribute('href')
    driver.get(link_infl)
    countries = ['Egypt' , 'Iraq' , 'Jordan' , 'Lebanon' , 'Libya' , 'Sudan' , 'Yemen' , 'Algeria' , 'Mauritania' , 'Somalia']
    from selenium.webdriver.support.ui import Select
    parent_node = driver.find_element(By.CLASS_NAME,'content-wrapper')
    select_element = parent_node.find_element(By.CLASS_NAME,'selectpicker')
    select = Select(select_element)
    option_list = select.options
    for i in countries:
        select.select_by_visible_text(i)
        time.sleep(5)
        displayed = driver.find_element(By.CLASS_NAME,'wrapper')
        content = parent_node.find_element(By.CLASS_NAME,'content')
        flex = content.find_element(By.CLASS_NAME,'row')
        flex.click()
        time.sleep(3)
        all_p = flex.find_elements(By.TAG_NAME,'p')
        url_Source = all_p[0].text.split(": ")[1]
        Source = url_Source.split('.')[1]
        description = all_p[1].text
        header = flex.find_element(By.CLASS_NAME,'box-header')
        # tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
        download = header.find_element(By.ID,'btnDownloadData')
        download.click()
        time.sleep(5)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        os.rename(file,f'{base_path}\\inflation {filename}')
        time.sleep(4)
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/inflation {filename}'
            BodyDict["JsonDetails"]["table"] = 'inflation' + ' ' + filename.split('.')[0] + ' ' + i  
            BodyDict["JsonDetails"]["description"] = description
            BodyDict["JsonDetails"]["topic"] =  'Inflation'
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = Source
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_Source

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
    select.select_by_visible_text('Syrian Arab Republic')
    displayed = driver.find_element(By.CLASS_NAME,'wrapper')
    content = parent_node.find_element(By.CLASS_NAME,'content')
    flex = content.find_element(By.CLASS_NAME,'row')
    flex.click()
    time.sleep(50)
    all_p = flex.find_elements(By.TAG_NAME,'p')
    url_Source = all_p[0].text.split(": ")[1]
    Source = url_Source.split('.')[1]
    description = all_p[1].text
    header = flex.find_element(By.CLASS_NAME,'box-header')
    tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
    download = header.find_element(By.ID,'btnDownloadData')
    download.click()
    time.sleep(3)
    file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    filename = os.path.split(file)[1]
    os.rename(file,f'{base_path}\\inflation {filename}')
    try :
        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/inflation {filename}'
        BodyDict["JsonDetails"]["table"] = 'inflation' + ' ' + filename.split('.')[0] + ' ' + i 
        BodyDict["JsonDetails"]["description"] = description
        BodyDict["JsonDetails"]["topic"] =  'Inflation'
        BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = Source
        BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_Source
        
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

    form = driver.find_element(By.ID,'form1')
    form.click()
    aside= form.find_element(By.CLASS_NAME,'main-sidebar.sidebar-dark-primary.elevation-4')
    aside.click()
    content_aside = aside.find_element(By.CLASS_NAME,'sidebar')
    ul = content_aside.find_element(By.TAG_NAME,'ul')
    all_li = ul.find_elements(By.TAG_NAME,'li')
    li = all_li[3]
    a = li.find_element(By.TAG_NAME,'a')
    a.click()
    ul_2 = li.find_element(By.CLASS_NAME,'nav.nav-treeview')
    all_li_2 = ul_2.find_elements(By.CLASS_NAME,'nav-item')
    a_exch = all_li_2[2].find_element(By.TAG_NAME,'a')
    link_exch = a_exch.get_attribute('href')
    driver.get(link_exch)
    countries = ['Egypt' , 'Iraq' , 'Jordan' , 'Lebanon' , 'Libya' , 'Sudan' , 'Yemen' , 'Algeria' , 'Mauritania' , 'Somalia']
    from selenium.webdriver.support.ui import Select
    parent_node = driver.find_element(By.CLASS_NAME,'content-wrapper')
    select_element = parent_node.find_element(By.CLASS_NAME,'selectpicker')
    select = Select(select_element)
    option_list = select.options
    for i in countries:
        select.select_by_visible_text(i)
        time.sleep(5)
        displayed = driver.find_element(By.CLASS_NAME,'wrapper')
        content = parent_node.find_element(By.CLASS_NAME,'content')
        flex = content.find_element(By.CLASS_NAME,'row')
        flex.click()
        time.sleep(3)
        all_p = flex.find_elements(By.TAG_NAME,'p')
        url_Source = all_p[0].text.split(": ")[1]
        Source = url_Source.split('.')[1]
        description = all_p[1].text.replace('\n',': ')
        header = flex.find_element(By.CLASS_NAME,'box-header')
        # tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
        download = header.find_element(By.ID,'btnDownloadData')
        download.click()
        time.sleep(5)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        os.rename(file,f'{base_path}\\Exchange Rate {filename}')
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/Exchange Rate {filename}'
            BodyDict["JsonDetails"]["table"] = 'Exchange Rate' + ' ' + filename.split('.')[0] + ' ' + i 
            BodyDict["JsonDetails"]["description"] = description
            BodyDict["JsonDetails"]["topic"] =  'Inflation'
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = Source
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_Source
            
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
        time.sleep(4)
    select.select_by_visible_text('Syrian Arab Republic')
    displayed = driver.find_element(By.CLASS_NAME,'wrapper')
    content = parent_node.find_element(By.CLASS_NAME,'content')
    flex = content.find_element(By.CLASS_NAME,'row')
    flex.click()
    time.sleep(50)
    all_p = flex.find_elements(By.TAG_NAME,'p')
    url_Source = all_p[0].text.split(": ")[1]
    Source = url_Source.split('.')[1]
    description = all_p[1].text.replace('\n',': ')
    header = flex.find_element(By.CLASS_NAME,'box-header')
    tools = header.find_element(By.CLASS_NAME,'pull-right.box-tools')
    download = header.find_element(By.ID,'btnDownloadData')
    download.click()
    time.sleep(3)
    file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    filename = os.path.split(file)[1]
    os.rename(file,f'{base_path}\\Exchange Rate {filename}')
    try :
        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/Exchange Rate {filename}'
        BodyDict["JsonDetails"]["table"] = 'Exchange Rate' + ' ' + filename.split('.')[0] + ' ' + i 
        BodyDict["JsonDetails"]["description"] = description
        BodyDict["JsonDetails"]["topic"] =  'Inflation'
        BodyDict["JsonDetails"]["tags"][0]["name"] = 'Economic'
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = Source
        BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_Source
        
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

    result = requests.get('https://dataviz.vam.wfp.org/seasonal_explorer/rainfall_vegetation/help')
    soup = BeautifulSoup(result.content,'html.parser')
    content = soup.find(class_='content h100')
    p = content.find_all('p')
    limitation = p[1].text.replace('\r\n                            ','').replace('\n','')
    limitation = limitation.split(':    ')[1]
    source_RF = p[2].text.replace('\r\n                            ','').replace('\n','')
    source_RF = source_RF.split(':    ')[1]
    p1 = p[3].text.replace('\r\n                            ','').replace('\n','')
    p3 = p[4].text.replace('\r\n                            ','').replace('\n','')
    p4 = p[5].text.replace('\r\n                            ','').replace('\n','')
    ul = content.find('ul')
    p2 = ul.text.replace('\n',' ; ')
    source_NDVI = p1 + ' ' + p2 + ' ' + p3 + ' ' + p4
    source_NDVI = source_NDVI.split(':    ')[1]

    countries = ['Egypt' ,'Bahrain' , 'Kuwait' , 'Morocco' , 'Oman' , 'Qatar' , 'Saudi Arabia' , 'Tunisia' , 'Syrian Arab Republic' , 'United Arab Emirates' , 'Iraq' , 'Jordan' , 'Lebanon' , 'Libya' , 'Sudan' , 'Yemen' , 'Algeria' , 'Mauritania' , 'Somalia']
    driver.get('https://dataviz.vam.wfp.org/seasonal_explorer/rainfall_vegetation/visualizations')
    from selenium.webdriver.support.ui import Select
    parent_node = driver.find_element(By.CLASS_NAME,'content-wrapper')
    select_element = parent_node.find_element(By.CLASS_NAME,'selectpicker')
    select = Select(select_element)
    option_list = select.options
    for i in countries:
        select.select_by_visible_text(i)
        displayed = driver.find_element(By.CLASS_NAME,'wrapper')
        content = parent_node.find_element(By.CLASS_NAME,'content')
        flexs = content.find_elements(By.CLASS_NAME,'row')
        flexs[0].click()
        section_rain = flexs[0].find_element(By.XPATH,'//*[@id="SecRfhChart"]')
        topic = section_rain.find_element(By.TAG_NAME,'h3').text
        box = section_rain.find_element(By.CLASS_NAME,'box.box-solid')
        header = box.find_element(By.CLASS_NAME,'box-header')
        download = header.find_element(By.ID,'LnkDownloadCsvRfh')
        download.click()
        time.sleep(3)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{filename}'
            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["topic"] =  topic
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Climate'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_RF
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = ''
            BodyDict["JsonDetails"]["limitations"] = limitation
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
        flexs[0].click()
        section_rain_anom = flexs[0].find_element(By.XPATH,'//*[@id="SecRfhAnomalyChart"]')
        topic = section_rain_anom.find_element(By.TAG_NAME,'h3').text
        box = section_rain_anom.find_element(By.CLASS_NAME,'box.box-solid')
        header = box.find_element(By.CLASS_NAME,'box-header')
        download = header.find_element(By.ID,'LnkDownloadCsvRfhAnom')
        download.click()
        time.sleep(3)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{filename}'
            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["topic"] =  topic
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Climate'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_RF
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = ''
            BodyDict["JsonDetails"]["limitations"] = limitation

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


        flexs[1].click()
        section_vi = flexs[0].find_element(By.XPATH,'//*[@id="SecVimChart"]')
        topic = section_vi.find_element(By.TAG_NAME,'h3').text
        box = section_vi.find_element(By.CLASS_NAME,'box.box-solid')
        header = box.find_element(By.CLASS_NAME,'box-header')
        download = header.find_element(By.ID,'LnkDownloadCsvVim')
        download.click()
        time.sleep(3)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{filename}'
            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["topic"] =  topic
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Climate'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_NDVI
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = ''
            BodyDict["JsonDetails"]["limitations"] = limitation
            
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
        flexs[1].click()
        section_anom = flexs[1].find_element(By.XPATH,'//*[@id="SecVimAnomalyChart"]')
        topic = section_anom.find_element(By.TAG_NAME,'h3').text
        box = section_anom.find_element(By.CLASS_NAME,'box.box-solid')
        header = box.find_element(By.CLASS_NAME,'box-header')
        download = header.find_element(By.ID,'LnkDownloadCsvVimAnom')
        download.click()
        time.sleep(3)
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        try :
            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/WFP/{filename}'
            BodyDict["JsonDetails"]["table"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["description"] = filename.split('.')[0]
            BodyDict["JsonDetails"]["topic"] =  topic
            BodyDict["JsonDetails"]["tags"][0]["name"] = 'Climate'
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_NDVI
            BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = ''
            BodyDict["JsonDetails"]["limitations"] = limitation
            
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
base_path = "//10.30.31.77/data_collection_dump/RawData/UNAIDS_FS"
import logging
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
from Hashing.HashScrapedData import _hashing
import time
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs\\UNAIDS_FactS.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "//10.30.31.77/data_collection_dump/RawData/UNAIDS_FS" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://aidsinfo.unaids.org/?did=5fbeb5cb94831556e11c4517&r=world&t=null&tb=q&bt=undefined&ts=0,0&qla=G&qls=All%20Countries')

    BodyDict = {
                "JobPath":f"", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNAIDS",
                        "source_description" : "The Joint United Nations Programme on HIV and AIDS is the main advocate for accelerated, comprehensive and coordinated global action on the HIV/AIDS pandemic.",
                        "source_url" : "https://unaids.org/en",
                        "table" : '',
                        "description" : '',
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
                            "name": "",
                            "url" : ""
                        }],
                        "limitations":"",
                        "concept":  "",
                        "periodicity":  "",
                        "topic":  '',
                        "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified": "",                #* ""               ""                  ""              ""
                        "TriggerTalend" : False,    #* initialise to True for production
                        "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/UNAIDS_FS" #* initialise as empty string for production.
                    }
                }

    #resource 1
    fs_type = driver.find_element(By.CLASS_NAME,'factsheet-type').text
    content = driver.find_element(By.CLASS_NAME,'quicklinks-content-table.col-xs-12.col-md-11')
    rows = content.find_elements(By.CLASS_NAME,'row')
    for row in rows:
        group = row.find_element(By.CLASS_NAME,'col-md-12.col-sm-12').find_element(By.TAG_NAME,'label')
        title = group.text + ' - ' + fs_type
        table = row.find_element(By.CLASS_NAME,'table.quicklinks-data-table')
        rows = table.find_elements(By.TAG_NAME,'tr')
        l = []
        for tr in rows:
            cells = tr.find_elements(By.TAG_NAME,'td')
            cells_text = [cell.text for cell in cells]
            l.append(cells_text)
        df = pd.DataFrame(l,columns=["categorie", "value"]) 
        df.to_csv(f'{base_path}/{title}.csv',index = False)
        try:
            BodyDict["JobPath"] = f'{base_path}/{title}.csv'
            BodyDict["JsonDetails"]["table"] = title
            BodyDict["JsonDetails"]["description"] = title
            BodyDict["JsonDetails"]["tags"][0]["name"] = "FACT SHEETS"
            BodyDict["JsonDetails"]["topic"] = title
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

    # resource 2
    from selenium import webdriver
    resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
    resource_2 = resources[1].click()
    time.sleep(1)
    dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog')
    webdriver.ActionChains(driver).move_to_element(dialog).perform()
    list_region = dialog.find_element(By.CLASS_NAME,'popup-content')
    regions = list_region.find_elements(By.TAG_NAME,'li')
    for j in range(len(regions)):
        regions[j].click()
        view_data = dialog.find_element(By.ID,'btnViewqlData')
        view_data.click()
        time.sleep(2)
        fs_type = driver.find_element(By.CLASS_NAME,'ql-selected-area-wrapper').find_element(By.TAG_NAME,'label').text
        content = driver.find_element(By.CLASS_NAME,'quicklinks-content-table.col-xs-12.col-md-11')
        rows = content.find_elements(By.CLASS_NAME,'row')
        for row in rows:
            group = row.find_element(By.CLASS_NAME,'col-md-12.col-sm-12').find_element(By.TAG_NAME,'label')
            title = group.text + ' - ' + fs_type + ' 2021'
            table = row.find_element(By.CLASS_NAME,'table.quicklinks-data-table')
            rows = table.find_elements(By.TAG_NAME,'tr')
            l = []
            for tr in rows:
                cells = tr.find_elements(By.TAG_NAME,'td')
                cells_text = [cell.text for cell in cells]
                l.append(cells_text)
            df = pd.DataFrame(l,columns=["categorie", "value"]) 
            df.to_csv(f'{base_path}/{title}.csv',index = False)
            try:
                BodyDict["JobPath"] = f'{base_path}/{title}.csv'
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = title
                BodyDict["JsonDetails"]["tags"][0]["name"] = "FACT SHEETS"
                BodyDict["JsonDetails"]["topic"] = title
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
        resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
        resource_2 = resources[1].click()
        time.sleep(1)
        dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog')
        webdriver.ActionChains(driver).move_to_element(dialog).perform()
        list_region = dialog.find_element(By.CLASS_NAME,'popup-content')
        regions = list_region.find_elements(By.TAG_NAME,'li')
    close = dialog.find_element(By.CLASS_NAME,'close.qlarealist')
    close.click()

    # resource 3
    resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
    resource_3 = resources[2].click()
    time.sleep(1)
    dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog.national')
    webdriver.ActionChains(driver).move_to_element(dialog).perform()
    cont = dialog.find_element(By.CLASS_NAME,'popup-content')
    countries = cont.find_elements(By.TAG_NAME,'li')
    for j in range(len(countries)):
        countries[j].click()
        view_data = dialog.find_element(By.ID,'btnViewqlData')
        view_data.click()
        time.sleep(2)
        fs_type = driver.find_element(By.CLASS_NAME,'ql-selected-area-wrapper').find_element(By.TAG_NAME,'label').text
        content = driver.find_element(By.CLASS_NAME,'quicklinks-content-table.col-xs-12.col-md-11')
        rows = content.find_elements(By.CLASS_NAME,'row')
        for row in rows:
            group = row.find_element(By.CLASS_NAME,'col-md-12.col-sm-12').find_element(By.TAG_NAME,'label')
            title = group.text + ' - ' + fs_type + ' 2021'
            table = row.find_element(By.CLASS_NAME,'table.quicklinks-data-table')
            rows = table.find_elements(By.TAG_NAME,'tr')
            l = []
            for tr in rows:
                cells = tr.find_elements(By.TAG_NAME,'td')
                cells_text = [cell.text for cell in cells]
                l.append(cells_text)
            if len(l[0]) == 2:
                df = pd.DataFrame(l,columns=["categorie", "value"]) 
            elif len(l[0]) == 3:
                df = pd.DataFrame(l,columns=["categorie", "value", "Source"]) 
            df.to_csv(f'{base_path}/{title}.csv',index = False)
            try:
                BodyDict["JobPath"] = f'{base_path}/{title}.csv'
                BodyDict["JsonDetails"]["table"] = title
                BodyDict["JsonDetails"]["description"] = title
                BodyDict["JsonDetails"]["tags"][0]["name"] = "FACT SHEETS"
                BodyDict["JsonDetails"]["topic"] = title
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
        resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
        resource_3 = resources[2].click()
        time.sleep(1)
        dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog')
        webdriver.ActionChains(driver).move_to_element(dialog).perform()
        cont = dialog.find_element(By.CLASS_NAME,'popup-content')
        countries = cont.find_elements(By.TAG_NAME,'li')
    close = dialog.find_element(By.CLASS_NAME,'close.qlarealist')
    close.click()

    # resource 4
    resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
    resource_4 = resources[3].click()
    time.sleep(1)
    dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog.national')
    webdriver.ActionChains(driver).move_to_element(dialog).perform()
    cont = dialog.find_element(By.CLASS_NAME,'popup-content')
    countries = cont.find_elements(By.TAG_NAME,'li')
    for j in range(len(countries)):
        countries[j].click()
        view_data = dialog.find_element(By.ID,'btnViewqlData')
        view_data.click()
        time.sleep(2)
        infos = driver.find_element(By.CLASS_NAME,'factsheet-type.country-brief-type').find_elements(By.TAG_NAME,'tr')
        title = infos[0].text.replace(':',' ')
        content = driver.find_element(By.CLASS_NAME,'quicklinks-content-table.col-xs-12.col-md-11')
        table = content.find_element(By.CLASS_NAME,'table.country-brief-content-table')
        rows = table.find_elements(By.TAG_NAME,'tr')
        l = []
        for tr in rows[1:]:
            cells = tr.find_elements(By.TAG_NAME,'td')
            cells_text = [cell.text for cell in cells]
            l.append(cells_text[:-1])
        df = pd.DataFrame(l,columns=["Indicator", "2010", "2021"]) 
        df['2021'] = df['2021'].str.replace('\n', ' ')
        df['2010'] = df['2010'].str.replace('\n', ' ')
        df.to_csv(f'{base_path}/{title}.csv',index = False)
        try:
            BodyDict["JobPath"] = f'{base_path}/{title}.csv'
            BodyDict["JsonDetails"]["table"] = title
            BodyDict["JsonDetails"]["description"] = title
            BodyDict["JsonDetails"]["tags"][0]["name"] = "FACT SHEETS"
            BodyDict["JsonDetails"]["topic"] = title
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
        resources = driver.find_element(By.CLASS_NAME,'list-group.panel').find_elements(By.CSS_SELECTOR,'a[data-parent="#MainMenu"]')[19:23]
        resource_4 = resources[3].click()
        time.sleep(1)
        dialog = driver.find_element(By.CLASS_NAME,'modal.fade.in').find_element(By.CLASS_NAME,'modal-dialog')
        webdriver.ActionChains(driver).move_to_element(dialog).perform()
        cont = dialog.find_element(By.CLASS_NAME,'popup-content')
        countries = cont.find_elements(By.TAG_NAME,'li')
    close = dialog.find_element(By.CLASS_NAME,'close.qlarealist')
    close.click()
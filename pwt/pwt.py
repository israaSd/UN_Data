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

from Hashing.HashScrapedData import _hashing
import shutil
import time
import os
import pandas as pd
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
import logging
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\pwt'

def execute():

    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/pwt_out.log"),
                                logging.StreamHandler()], level=logging.INFO)

    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\pwt" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://www.rug.nl/ggdc/productivity/pwt/pwt-releases/pwt100')

    accept_all_cookies = driver.find_element(By.CLASS_NAME,'rug-button.rug-button--secondary.rug-button--medium.rug-mr.js--cookie')
    accept_all_cookies.click()

    BodyDict = {
                "JobPath":f"", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "third-parties",
                        "source": "Penn-World-Table",
                        "source_description" : "The Penn World Table (PWT) is a set of national-accounts data developed and maintained by scholars at the University of California, Davis and the Groningen Growth Development Centre of the University of Groningen to measure real GDP across countries and over time.",
                        "source_url" : "https://www.rug.nl/ggdc/productivity/pwt/",
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
                        "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData" #* initialise as empty string for production.
                    }
                }

    desc_icp = 'PPPs and expenditure at the most detailed level for publicly available benchmark years'
    cell_ICP_benchmark_data = driver.find_element(By.XPATH,"//*[contains(text(), 'ICP benchmark data')]")
    ICP_benchmark_datas = cell_ICP_benchmark_data.find_elements(By.TAG_NAME,'a')
    for data in ICP_benchmark_datas:
        data.click()
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
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
        filename = os.path.split(file)[1]
        filename = filename.split('.')[0]
        if filename == 'icp1996':
            xls = pd.ExcelFile(f'{base_path}\\icp1996.xls')
            sheets = xls.sheet_names
            df_1 = pd.read_excel(f'{base_path}\\icp1996.xls',sheets[0])
            topic = df_1.columns[0]
            df_1.columns = df_1.iloc[0,:]
            df_1.rename(columns = {df_1.columns[0]:'Material'}, inplace = True)
            df_1 = df_1[2:]
            title = topic.split('(')[0]
            df_1.to_csv(f'{base_path}\\icp1996 {title}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/icp1996 {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'icp1996 {title}'
                BodyDict["JsonDetails"]["description"] = topic
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'icp1996 benchmark data - {topic}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_2 = pd.read_excel(f'{base_path}\\icp1996.xls',sheets[1])
            topic = df_2.columns[0]
            df_2.columns = df_2.iloc[1,:]
            df_2.rename(columns = {df_2.columns[0]:'Material'}, inplace = True)
            df_2 = df_2[2:]
            title = topic.split('(')[0]
            df_2.to_csv(f'{base_path}\\icp1996 {title}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/icp1996 {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'icp1996 {title}'
                BodyDict["JsonDetails"]["description"] = topic
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'icp1996 benchmark data - {topic}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_3 = pd.read_excel(f'{base_path}\\icp1996.xls',sheets[2])
            title = sheets[2]
            df_3.to_csv(f'{base_path}\\icp1996 {sheets[2]}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/icp1996 {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'icp1996 {title}'
                BodyDict["JsonDetails"]["description"] = f'icp1996 benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'icp1996 benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_4 = pd.read_excel(f'{base_path}\\icp1996.xls',sheets[3])
            title = sheets[3]
            df_4.to_csv(f'{base_path}\\icp1996 {sheets[3]}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/icp1996 {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'icp1996 {title}'
                BodyDict["JsonDetails"]["description"] = f'icp1996 benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'icp1996 benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
        else:
            xls = pd.ExcelFile(f'{base_path}\\{filename}.xls')
            sheets = xls.sheet_names
            df_1 = pd.read_excel(f'{base_path}\\{filename}.xls',sheets[0])
            title = sheets[0]
            df_1.to_csv(f'{base_path}\\{filename} {title}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename} {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'{filename} {title}'
                BodyDict["JsonDetails"]["description"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_2 = pd.read_excel(f'{base_path}\\{filename}.xls',sheets[1])
            title = df_2.columns[0]
            file_n = title.split('(')[0]
            df_2.columns = df_2.iloc[0,:]
            df_2 = df_2[1:]
            df_2.to_csv(f'{base_path}\\{filename} {file_n}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename} {file_n}.csv"
                BodyDict["JsonDetails"]["table"] = f'{filename} {file_n}'
                BodyDict["JsonDetails"]["description"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_3 = pd.read_excel(f'{base_path}\\{filename}.xls',sheets[2])
            title = df_3.columns[0]
            file_n = title.split('(')[0]
            df_3.columns = df_3.iloc[0,:]
            df_3 = df_3[1:]
            df_3.to_csv(f'{base_path}\\{filename} {file_n}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename} {file_n}.csv"
                BodyDict["JsonDetails"]["table"] = f'{filename} {file_n}'
                BodyDict["JsonDetails"]["description"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_4 = pd.read_excel(f'{base_path}\\{filename}.xls',sheets[3])
            title = df_4.columns[0]
            file_n = title.split('(')[0]
            df_4.columns = df_4.iloc[0,:]
            df_4 = df_4[1:]
            df_4.to_csv(f'{base_path}\\{filename} {file_n}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename} {file_n}.csv"
                BodyDict["JsonDetails"]["table"] = f'{filename} {file_n}'
                BodyDict["JsonDetails"]["description"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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
            df_5 = pd.read_excel(f'{base_path}\\{filename}.xls',sheets[4])
            title = sheets[4]
            df_5.to_csv(f'{base_path}\\{filename} {sheets[4]}.csv',index=False, encoding="utf-8")
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename} {title}.csv"
                BodyDict["JsonDetails"]["table"] = f'{filename} {title}'
                BodyDict["JsonDetails"]["description"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP benchmark data'
                BodyDict["JsonDetails"]["topic"] = f'{filename} benchmark data - {title}'
                BodyDict["JsonDetails"]["concept"] = desc_icp
                
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

    links_pwt = []
    list_pwt = driver.find_element(By.CLASS_NAME,'rug-nav--secondary__sub.js--togglable-item.rug-block').find_elements(By.TAG_NAME,'li')
    for item in list_pwt[1:]:
        pwt_e_r = item.find_element(By.TAG_NAME,'a').get_attribute('href')
        links_pwt.append(pwt_e_r)

    for link in links_pwt:
        driver.get(link)
        time.sleep(2)
        if link == links_pwt[0]:
            cell_RGDP = driver.find_element(By.XPATH,"//*[contains(text(), 'RGDP')]")
            cell_RGDP.click()
            time.sleep(2)
            driver.switch_to.window(driver.window_handles[1])
            title_f = driver.find_element(By.CLASS_NAME,'portalSubLogo').text
            excel = driver.find_element(By.CLASS_NAME,'mk3menu').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a')
            excel.click()
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
            definitions = []
            xls = pd.ExcelFile(f'{base_path}\\FebPwtExport3302023.xlsx')
            sheets = xls.sheet_names
            df_data = pd.read_excel(f'{base_path}\\FebPwtExport3302023.xlsx',sheets[1])
            df_data.to_csv(f'{base_path}\\{title_f}.csv',index=False,encoding="utf-8")
            df_desc = pd.read_excel(f'{base_path}\\FebPwtExport3302023.xlsx',sheets[2])
            df_desc.dropna(axis='columns', how='all')
            df_desc = df_desc.dropna(how='all', axis=1)
            for i in range(len(df_desc.index)):
                f = df_desc.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                definitions.append(f[0])
            data_def = [' \n\n '.join(map(str,definitions))][0]
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{title_f}.csv"
                BodyDict["JsonDetails"]["table"] = f'{title_f}'
                BodyDict["JsonDetails"]["description"] = data_def
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'ICP'
                BodyDict["JsonDetails"]["topic"] = title_f
                BodyDict["JsonDetails"]["concept"] = 'Real GDP variables are in (millions) of US dollars, not in per-capita terms. A population variable (acronym pop), which can be used to convert GDP into per-capita terms, is included in the relevant queries.'
                
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
            driver.switch_to.window(driver.window_handles[0])

        try:
            import datetime
            update = driver.find_element(By.TAG_NAME,'em').text
            try:
                update = update.split('on ')[1].split(', P')[0].replace(',','')
                update = datetime.datetime.strptime(update, "%B %d %Y").strftime('%Y-%m-%d')
            except:
                update = update.split('On ')[1].split(' w')[0].replace(',','')
                update = datetime.datetime.strptime(update, "%B %d %Y").strftime('%Y-%m-%d')
        except:
            update = ''
        main = driver.find_element(By.ID,'main')
        header = main.find_element(By.CLASS_NAME,'rug-panel--content.rug-panel--content--border')
        tag = header.find_element(By.TAG_NAME,'h1').text
        try:
            topic = header.find_element(By.TAG_NAME,'div').text
        except:
            t = tag.split(' ')[1]
            topic = f'Penn World Table version {t}'
        concept_data = main.find_element(By.CLASS_NAME,'rug-clearfix.rug-theme--content.rug-mb').text.split('. For qu')[0]
        try:
            concept_data = main.find_element(By.CLASS_NAME,'rug-clearfix.rug-theme--content.rug-mb').text.split('. Access')[0]
        except:
            pass
        try:
            concept_data = main.find_element(By.CLASS_NAME,'rug-clearfix.rug-theme--content.rug-mb').text.split('. It')[0]
        except:
            pass
        data = main.find_element(By.CLASS_NAME,'rug-mb-s').find_element(By.TAG_NAME,'a')
        data.click()
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
        try :
            import pandas as pd
            definitions = []
            xls = pd.ExcelFile(file)
            sheets = xls.sheet_names
            df_data = pd.read_excel(file,sheets[-1])
            df_data.to_csv(f'{base_path}\\{tag}.csv',index=False, encoding="utf-8")
            df_desc = pd.read_excel(file,sheets[-2])
            for i in range(len(df_desc.index)):
                f = df_desc.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                definitions.append(f[0])
            data_def = [' \n\n '.join(map(str,definitions))][0]
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{tag}.csv"
                BodyDict["JsonDetails"]["table"] = f'{tag}'
                BodyDict["JsonDetails"]["description"] = data_def
                BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                BodyDict["JsonDetails"]["topic"] = topic
                BodyDict["JsonDetails"]["created"] =  update
                BodyDict["JsonDetails"]["last_modified"] =  update
                BodyDict["JsonDetails"]["concept"] = concept_data
                
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
        except:
            from zipfile import ZipFile
            if filename.endswith(".zip"):
                if '63' in filename:
                    name = ZipFile(fr'{base_path}\\{filename}').namelist()
                    data_file = name[0]
                    defin_file = name[1]
                    shutil.unpack_archive(fr"{base_path}\{filename}", base_path)
                    print(data_file)
                    df_desc = pd.read_excel(f'{base_path}\\{defin_file}')
                    for i in range(len(df_desc.index)):
                        f = df_desc.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        definitions.append(f[0])
                    data_def = [' \n\n '.join(map(str,definitions))][0]
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{data_file}"
                        BodyDict["JsonDetails"]["table"] = f'{tag}'
                        BodyDict["JsonDetails"]["description"] = data_def
                        BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                        BodyDict["JsonDetails"]["topic"] = topic
                        BodyDict["JsonDetails"]["concept"] = concept_data
                        
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
                else:
                    name = ZipFile(fr'{base_path}\\{filename}').namelist()
                    data_file = name[1]
                    defin_file = name[0]
                    shutil.unpack_archive(fr"{base_path}\{filename}", base_path)
                    print(data_file)
                    df_desc = pd.read_excel(f'{base_path}\\{defin_file}')
                    for i in range(len(df_desc.index)):
                        f = df_desc.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        definitions.append(f[0])
                    data_def = [' \n\n '.join(map(str,definitions))][0]
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{data_file}"
                        BodyDict["JsonDetails"]["table"] = f'{tag}'
                        BodyDict["JsonDetails"]["description"] = data_def
                        BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                        BodyDict["JsonDetails"]["topic"] = topic
                        BodyDict["JsonDetails"]["concept"] = concept_data
                        
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
            else:
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename}"
                    BodyDict["JsonDetails"]["table"] = f'{tag}'
                    BodyDict["JsonDetails"]["description"] = data_def
                    BodyDict["JsonDetails"]["tags"][0]["name"] = tag
                    BodyDict["JsonDetails"]["topic"] = topic
                    BodyDict["JsonDetails"]["concept"] = concept_data
                    
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
        if link in links_pwt[0:2] :
            cell_NA_data = driver.find_element(By.XPATH,"//*[contains(text(), 'NA data')]")
            NA_data = cell_NA_data.find_element(By.TAG_NAME,'a')
            description_NA_data = 'The National Accounts data in current and constant national prices and exchange rate and population data'
            NA_data.click()
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
            try :
                import pandas as pd
                definitions = []
                xls = pd.ExcelFile(file)
                sheets = xls.sheet_names
                df_data = pd.read_excel(file,sheets[-1])
                df_data.to_csv(f'{base_path}\\na data {tag}.csv',index=False, encoding="utf-8")
                df_desc = pd.read_excel(file,sheets[-2])
                for i in range(len(df_desc.index)):
                    f = df_desc.iloc[i].to_list()
                    f = [' : '.join(map(str,f))]
                    definitions.append(f[0])
                data_def = [' \n\n '.join(map(str,definitions))][0]

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                    BodyDict["JsonDetails"]["description"] = data_def
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                    BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_NA_data
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
            except:
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                    BodyDict["JsonDetails"]["description"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                    BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_NA_data
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
            cell_capital_detail = driver.find_element(By.XPATH,"//*[contains(text(), 'Capital detail')]")
            capital_detail = cell_capital_detail.find_element(By.TAG_NAME,'a')
            description_capital_detail = 'Investment, capital stock and capital consumption data by assets.'
            capital_detail.click()
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
            try :
                import pandas as pd
                definitions = []
                xls = pd.ExcelFile(file)
                sheets = xls.sheet_names
                df_data = pd.read_excel(file,sheets[-1])
                df_data.to_csv(f'{base_path}\\Capital detail {tag}.csv',index=False, encoding="utf-8")
                df_desc = pd.read_excel(file,sheets[-2])
                for i in range(len(df_desc.index)):
                    f = df_desc.iloc[i].to_list()
                    f = [' : '.join(map(str,f))]
                    definitions.append(f[0])
                data_def = [' \n\n '.join(map(str,definitions))][0]
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/Capital detail {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'Capital detail {tag}'
                    BodyDict["JsonDetails"]["description"] = data_def
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'Capital detail'
                    BodyDict["JsonDetails"]["topic"] = f'Capital detail {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_capital_detail
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
            except:
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/Capital detail {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'Capital detail {tag}'
                    BodyDict["JsonDetails"]["description"] = f'Capital detail {topic}'
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'Capital detail'
                    BodyDict["JsonDetails"]["topic"] = f'Capital detail {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_capital_detail
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
        elif link in links_pwt[2:6] :
            cell_NA_data = driver.find_element(By.XPATH,"//*[contains(text(), 'NA data')]")
            NA_data = cell_NA_data.find_elements(By.TAG_NAME,'a')[1]
            description_NA_data = 'The National Accounts data in current and constant national prices and exchange rate and population data'
            NA_data.click()
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
            try :
                import pandas as pd
                definitions = []
                xls = pd.ExcelFile(file)
                sheets = xls.sheet_names
                df_data = pd.read_excel(file,sheets[-1])
                df_data.to_csv(f'{base_path}\\na data {tag}.csv',index=False, encoding="utf-8")
                df_desc = pd.read_excel(file,sheets[-2])
                for i in range(len(df_desc.index)):
                    f = df_desc.iloc[i].to_list()
                    f = [' : '.join(map(str,f))]
                    definitions.append(f[0])
                data_def = [' \n\n '.join(map(str,definitions))][0]
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                    BodyDict["JsonDetails"]["description"] = data_def
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                    BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_NA_data
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
            except:
                try:
                    os.rename(file,f"{base_path}//na data {tag}.csv")
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.csv"
                    BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                    BodyDict["JsonDetails"]["description"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                    BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_NA_data
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
            try :
                cell_Depreciation_rates = driver.find_element(By.XPATH,"//*[contains(text(), 'Depreciation rates')]")
                Depreciation_rates = cell_Depreciation_rates.find_elements(By.TAG_NAME,'a')[1]
                description_Depreciation_rates = 'The depreciation of the overall capital stock, by country and year'
                Depreciation_rates.click()
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
                file_n = filename.split('.')[0]
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/{filename}"
                    BodyDict["JsonDetails"]["table"] = f'{file_n} {tag}'
                    BodyDict["JsonDetails"]["description"] = description_Depreciation_rates
                    BodyDict["JsonDetails"]["tags"][0]["name"] = 'Depreciation rates'
                    BodyDict["JsonDetails"]["topic"] = f'Depreciation rates {topic}'
                    BodyDict["JsonDetails"]["concept"] = description_Depreciation_rates
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
            except:
                pass
        elif link in links_pwt[6:10]:
            table_add_data = driver.find_element(By.CLASS_NAME,'rug-table--default')
            na_data = table_add_data.find_elements(By.TAG_NAME,'tr')[-2].find_element(By.TAG_NAME,'a')
            na_data.click()
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
            file_na_data = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
            filename_na_data = os.path.split(file_na_data)[1]
            os.rename(file_na_data,f'{base_path}\\na data {tag}.xls')
            na_data_desc = table_add_data.find_elements(By.TAG_NAME,'tr')[-1].find_element(By.TAG_NAME,'a')
            na_data_desc.click()
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
            df_desc = pd.read_excel(file)
            for i in range(len(df_desc.index)):
                f = df_desc.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                definitions.append(f[0])
            data_def = [' \n\n '.join(map(str,definitions))][0]
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.xls"
                BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                BodyDict["JsonDetails"]["description"] = data_def
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                BodyDict["JsonDetails"]["concept"] = description_NA_data
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

        elif link in links_pwt[10]:
            table_add_data = driver.find_element(By.CLASS_NAME,'rug-table--default')
            na_data = table_add_data.find_elements(By.TAG_NAME,'tr')[1].find_element(By.TAG_NAME,'a')
            na_data.click()
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
            file_na_data = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
            filename_na_data = os.path.split(file_na_data)[1]
            os.rename(file_na_data,f'{base_path}\\na data {tag}.xls')
            print(filename)
            na_data_desc = table_add_data.find_elements(By.TAG_NAME,'tr')[2].find_element(By.TAG_NAME,'a')
            na_data_desc.click()
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
            df_desc = pd.read_excel(file)
            for i in range(len(df_desc.index)):
                f = df_desc.iloc[i].to_list()
                f = [' : '.join(map(str,f))]
                definitions.append(f[0])
            data_def = [' \n\n '.join(map(str,definitions))][0]
            try:
                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/pwt/na data {tag}.xls"
                BodyDict["JsonDetails"]["table"] = f'na data {tag}'
                BodyDict["JsonDetails"]["description"] = data_def
                BodyDict["JsonDetails"]["tags"][0]["name"] = 'NA data'
                BodyDict["JsonDetails"]["topic"] = f'na data {topic}'
                BodyDict["JsonDetails"]["concept"] = description_NA_data
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

# execute()
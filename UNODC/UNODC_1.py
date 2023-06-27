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
import urllib.request
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
import pandas as pd
import numpy as np
import logging
import datetime
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNODC_1_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    url = 'https://dataunodc.un.org/'
    driver.get(url)

    content = driver.find_element(By.ID,'wrapper')
    theme_2 = content.find_elements(By.CLASS_NAME,'theme')[1]
    tag_2 = theme_2.text
    click = theme_2.find_element(By.TAG_NAME,'a')
    theme_2.click()
    time.sleep(2)
    title_2 = driver.find_element(By.CLASS_NAME,'title').text
    dataset_2 = driver.find_element(By.CLASS_NAME,'rteright')

    link_metadata_2 = dataset_2.find_elements(By.TAG_NAME,'a')[1].get_attribute('href')
    urllib.request.urlretrieve(link_metadata_2,f"{base_path}\\{title_2}.pdf")
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
    file_pdf_2 = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    import pdfplumber
    import datetime
    conten_2=[]
    with pdfplumber.open(file_pdf_2) as pdf:
        text_2 = pdf.pages
        for te_2 in text_2:
            content_2 = te_2.extract_text()
            conten_2.append(content_2)
    content_all_2 = ' '.join([str(elem) for elem in conten_2])
    concept_2_pdf = content_all_2.split('Metadata Information')[1].split('Dataset characteristics')[0].replace('\n','')
    last_update_2_pdf = content_all_2.split('Last update: ')[1].split('Base period: ')[0].replace(' \n','')
    last_update_2_pdf = datetime.datetime.strptime(last_update_2_pdf, "%d/%m/%Y").strftime('%Y-%m-%d')
    source_2_pdf = content_all_2.split('Data source(s): ')[1].split('Contact')[0].replace(' \n','')
    defin_2_pdf = content_all_2.split('Statistical concepts and definitions')[1].split('Data sources and method of collection')[0].replace(' \n','')

    driver.back()
    time.sleep(2)
    content = driver.find_element(By.ID,'wrapper')
    theme = content.find_element(By.CLASS_NAME,'theme')
    tag_1 = theme.text
    click = theme.find_element(By.TAG_NAME,'a')
    theme.click()
    time.sleep(2)
    title = driver.find_element(By.CLASS_NAME,'title').text
    dataset = driver.find_element(By.CLASS_NAME,'rteright')

    link_metadata = dataset.find_elements(By.TAG_NAME,'a')[1].get_attribute('href')
    urllib.request.urlretrieve(link_metadata,f"{base_path}\\{title}.pdf")
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
    file_pdf = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    import pdfplumber
    import datetime
    conten=[]
    with pdfplumber.open(file_pdf) as pdf:
        text = pdf.pages
        for te in text:
            content = te.extract_text()
            conten.append(content)
    content_all = ' '.join([str(elem) for elem in conten])
    concept_pdf = content_all.split('Metadata Information')[1].split('Dataset characteristics')[0].replace('\n','')
    last_update_pdf = content_all.split('Last update: ')[1].split('Base period: ')[0].replace(' \n','')
    last_update_pdf = datetime.datetime.strptime(last_update_pdf, "%d/%m/%Y").strftime('%Y-%m-%d')
    source_pdf = content_all.split('Data source(s): ')[1].split('Contact')[0].replace(' \n','')
    defin_pdf = content_all.split('Statistical concepts and definitions')[1].split('Data sources and method of collection')[0].replace(' \n','')

    topics_name = []
    dataset = driver.find_element(By.CLASS_NAME,'rteright')
    link_data = dataset.find_element(By.TAG_NAME,'a')
    link_data.click()
    time.sleep(2)
    topics = driver.find_elements(By.CLASS_NAME,'col-lg-12')
    topics = topics[2:8] + topics[10:12]
    time.sleep(1)
    for topic in topics:
        topic_name = topic.find_element(By.TAG_NAME,'h5').text.split('.')[1].split('(')[0]
        topics_name.append(topic_name)
        datasets  = topic.find_elements(By.TAG_NAME,'a')
        for d in datasets:
            d.click()
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

    # topic 8
    topics = driver.find_elements(By.CLASS_NAME,'col-lg-12')

    dataset_8  = topics[9].find_elements(By.TAG_NAME,'a')
    topic_name_8 = topics[9].find_element(By.TAG_NAME,'h5').text.split('.')[1].split('(')[0]
    dataset_8 = [dataset_8[0]]  + [dataset_8[2]]
    for d_8 in dataset_8:
        d_8.click()
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
            else:
                fileends = "None"
        latest_download_file() 
        file = max([base_path + "\\" + f for f in os.listdir(base_path)],key=os.path.getctime)
    driver.back()
    time.sleep(2)
    dataset = driver.find_element(By.CLASS_NAME,'rteright')
    link_data = dataset.find_element(By.TAG_NAME,'a')
    link_data.click()
    time.sleep(2)
    # topic 7
    # driver.get(driver.current_url)
    topics = driver.find_elements(By.CLASS_NAME,'col-lg-12')
    topic_name_7 = topics[8].find_element(By.TAG_NAME,'h5').text.split('.')[1].split('(')[0]
    dataset_7  = topics[8].find_element(By.TAG_NAME,'a')
    dataset_7.click()
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



    driver.back()
    driver.back()
    time.sleep(2)

    BodyDict = {
                "JobPath":f"", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNODC",
                        "source_description" : "The UNODC thematic programme on research, trend analysis and forensics undertakes  thematic research programmes, manages global and regional data collections, provides scientific and forensic services, defines research standards, and supports Member States to strengthen their data collection, research and forensics capacity.",
                        "source_url" : "https://dataunodc.un.org/",
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


    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.1_-_Prevalence_of_drug_use_in_the_general_population_regional_and_global_estimates.xlsx')
    df_1 = df[:28]
    title = df.iloc[0,0]
    df_1.iloc[1:4] = df_1.iloc[1:4].fillna(method='ffill', axis=1)
    df_1.iloc[1:4] = df_1.iloc[1:4].fillna('')
    df_1.columns = df_1.iloc[1:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    # df.iloc[1:27]
    df_1 = df_1[4:28]
    for i in df_1.columns:
        df_1.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    # df.columns
    df_2 = df[32:59]
    df_2.iloc[0:3] = df_2.iloc[0:3].fillna(method='ffill', axis=1)
    df_2.iloc[0:3] = df_2.iloc[0:3].fillna('')
    df_2.columns = df_2.iloc[0:3].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    for i in df_2.columns:
        df_2.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_data = df_1.merge(df_2,on='Region or subregion ')
    file_name = 'Prevalence of drug use in the general population regional and global estimates'
    df_data.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    desc = df.iloc[59,0] + ' ' + df.iloc[60,0] + ' ' + defin_pdf
    source = df.iloc[62,0] + ' ' + source_pdf
    topic_name_1 = topics_name[0]


    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = desc
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source  
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx')
    sheets = xls.sheet_names
    df_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[0])
    title_1 = df_1.columns[0]
    desc_1 = df_1.iloc[387,0]
    df_1.columns = df_1.iloc[1,:]
    for i in df_1.columns:
        df_1.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_1 = df_1[2:386]
    df_1[df_1.columns[0]] = df_1[df_1.columns[0]].ffill(axis = 0)
    df_1[df_1.columns[1]] = df_1[df_1.columns[1]].ffill(axis = 0)
    file_name = f'{title_1} - Prevalence of drug use in the general population including NPS national data'
    df_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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

    df_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[1])
    title_2 = df_2.columns[0].split('\n')[0]
    desc_2 = df_2.iloc[314,0]
    df_2.columns = df_2.iloc[1,:]
    for i in df_2.columns:
        df_2.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_2 = df_2[2:314]
    df_2[df_2.columns[0]] = df_2[df_2.columns[0]].ffill(axis = 0)
    df_2[df_2.columns[1]] = df_2[df_2.columns[1]].ffill(axis = 0)
    file_name = f'{title_2} - Prevalence of drug use in the general population including NPS national data'
    df_2.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_3 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[2])
    title_3 = df_3.columns[0].split('\n')[0]
    desc_3 = df_3.iloc[303,0]
    df_3.columns = df_3.iloc[1,:]
    for i in df_3.columns:
        df_3.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_3 = df_3[2:303]
    df_3[df_3.columns[0]] = df_3[df_3.columns[0]].ffill(axis = 0)
    df_3[df_3.columns[1]] = df_3[df_3.columns[1]].ffill(axis = 0)
    file_name = f'{title_3} - Prevalence of drug use in the general population including NPS national data'
    df_3.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_4 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[3])
    title_4 = df_4.columns[0].split('\n')[0]
    desc_4 = df_4.iloc[313,0]
    df_4.columns = df_4.iloc[1,:]
    for i in df_4.columns:
        df_4.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_4 = df_4[2:313]
    df_4[df_4.columns[0]] = df_4[df_4.columns[0]].ffill(axis = 0)
    df_4[df_4.columns[1]] = df_4[df_4.columns[1]].ffill(axis = 0)
    file_name = f'{sheets[3]} - Prevalence of drug use in the general population including NPS national data'
    df_4.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_5 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[4])
    title_5 = df_5.columns[0].split('\n')[0]
    desc_5 = df_5.iloc[46,0]
    df_5.columns = df_5.iloc[1,:]
    for i in df_5.columns:
        df_5.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_5 = df_5[2:46]
    df_5[df_5.columns[0]] = df_5[df_5.columns[0]].ffill(axis = 0)
    df_5[df_5.columns[1]] = df_5[df_5.columns[1]].ffill(axis = 0)
    file_name = f'{title_5} - Prevalence of drug use in the general population including NPS national data'
    df_5.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_6 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[5])
    title_6 = df_6.columns[0].split('\n')[0]
    desc_6 = df_6.iloc[123,0]
    df_6.columns = df_6.iloc[1,:]
    for i in df_6.columns:
        df_6.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_6 = df_6[2:123]
    df_6[df_6.columns[0]] = df_6[df_6.columns[0]].ffill(axis = 0)
    df_6[df_6.columns[1]] = df_6[df_6.columns[1]].ffill(axis = 0)
    file_name = f'{title_6} - Prevalence of drug use in the general population including NPS national data'
    df_6.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_7 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[6])
    title_7 = df_7.columns[0].split('\n')[0]
    desc_7 = df_7.iloc[277,0]
    df_7.columns = df_7.iloc[1,:]
    for i in df_7.columns:
        df_7.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_7 = df_7[2:277]
    df_7[df_7.columns[0]] = df_7[df_7.columns[0]].ffill(axis = 0)
    df_7[df_7.columns[1]] = df_7[df_7.columns[1]].ffill(axis = 0)
    file_name = f'{title_7} - Prevalence of drug use in the general population including NPS national data'
    df_7.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_8 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[7])
    title_8 = df_8.columns[0].split('\n')[0]
    desc_8 = df_8.iloc[91,0]
    df_8.columns = df_8.iloc[1,:]
    for i in df_8.columns:
        df_8.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_8 = df_8[2:91]
    df_8[df_8.columns[0]] = df_8[df_8.columns[0]].ffill(axis = 0)
    df_8[df_8.columns[1]] = df_8[df_8.columns[1]].ffill(axis = 0)
    file_name = f'{title_8} - Prevalence of drug use in the general population including NPS national data'
    df_8.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_9 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[8])
    title_9 = df_9.columns[0].split('\n')[0]
    desc_9 = df_9.iloc[154,0]
    df_9.columns = df_9.iloc[1,:]
    for i in df_9.columns:
        df_9.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_9 = df_9[2:154]
    df_9[df_9.columns[0]] = df_9[df_9.columns[0]].ffill(axis = 0)
    df_9[df_9.columns[1]] = df_9[df_9.columns[1]].ffill(axis = 0)
    file_name = f'{title_9} - Prevalence of drug use in the general population including NPS national data'
    df_9.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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
    df_10 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.2_-_Prevalence_of_drug_use_in_the_general_population_including_NPS_national_data.xlsx',sheets[9])
    title_10 = df_10.columns[0].split('\n')[0]
    df_10.iloc[1:3] = df_10.iloc[1:3].fillna(method='ffill', axis=1)
    df_10.iloc[1:3] = df_10.iloc[1:3].fillna('')
    df_10.columns = df_10.iloc[1:3].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    for i in df_10.columns:
        df_10.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df_10 = df_10[2:256]
    df_10[df_10.columns[0]] = df_10[df_10.columns[0]].ffill(axis = 0)
    df_10[df_10.columns[1]] = df_10[df_10.columns[1]].ffill(axis = 0)
    df_10[df_10.columns[2]] = df_10[df_10.columns[2]].ffill(axis = 0)
    file_name = f'{title_10} - Prevalence of drug use in the general population including NPS national data'
    df_10.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_pdf  
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.3_-_Prevalence_of_cannabis_use_in_the_population_1516_regional_and_global_estimates.xlsx')
    title = df.columns[0]
    df.iloc[1:3] = df.iloc[1:3].fillna(method='ffill', axis=1)
    df.iloc[1:3] = df.iloc[1:3].fillna('')
    df.columns = df.iloc[1:3].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    for i in df.columns:
        df.rename(columns = {i:i.replace('\n',' ')}, inplace = True)
    df.rename(columns = {'':'Region or subregion'}, inplace = True)
    source = df.iloc[28,0]+ '. ' + source_pdf 
    df = df[3:27]
    file_name = title
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\1.4_-_Prevalence_of_drug_use_in_the_youth_population_including_NPS_national_data.xlsx')
    title = df.columns[0] + ' ' + df.iloc[0,0] + ' ' + df.iloc[1,0]
    df.columns = df.iloc[2,:]
    desc = df.iloc[598,1] + ' '+ defin_pdf
    source = df.iloc[599,0] + ' ' + source_pdf
    df = df[3:597]
    file_name = title
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = desc
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topic_name_1
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\2.1._-_Expert_perception_of_trend_changes_in_used_of_drugs_all_drugs.xlsx')
    title = df.columns[0]
    df.columns = df.iloc[2,:]
    df = df.iloc[3:]
    file_name = title
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topics_name[1]
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source_pdf
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\3.1_-_Expert_perception_of_ranking_of_drugs_in_order_of_prevalence_of_drug_use.xlsx')
    sheets = xls.sheet_names
    for sheet in sheets:
        df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\3.1_-_Expert_perception_of_ranking_of_drugs_in_order_of_prevalence_of_drug_use.xlsx',sheet)
        title = df.columns[0]
        df.columns = df.iloc[1,:]
        df = df[2:]
        file_name = title
        df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
        try:
            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
            BodyDict["JsonDetails"]["table"] = file_name
            BodyDict["JsonDetails"]["description"] = defin_pdf
            BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
            BodyDict["JsonDetails"]["topic"] = topics_name[2]
            BodyDict["JsonDetails"]["created"] =  last_update_pdf
            BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
            BodyDict["JsonDetails"]["concept"] = concept_pdf
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source_pdf
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

    notes = []
    import pandas as pd
    import numpy as np
    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\4.1_-_Estimates_of_people_who_inject_drugs_living_with_HIV_HCV_HBV.xlsx')
    sheets = xls.sheet_names
    df_notes = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\4.1_-_Estimates_of_people_who_inject_drugs_living_with_HIV_HCV_HBV.xlsx',sheets[0])
    nte = df_notes.iloc[0,1] + '. '+ df_notes.iloc[2,2]+ '. ' 
    df_notes = df_notes.iloc[3:,2:]
    for i in range(len(df_notes.index)):
        f = df_notes.iloc[i].to_list()
        f = [' : '.join(map(str,f))]
        notes.append(f[0])
    note = nte + [' ,'.join(map(str,notes))][0]
    for k in range(1,5):
        df_data = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\4.1_-_Estimates_of_people_who_inject_drugs_living_with_HIV_HCV_HBV.xlsx',sheets[k])
        df_data.dropna(axis='columns', how='all')
        df_data = df_data.dropna(how='all', axis=1)
        df_data[0:4] = df_data.iloc[0:4].fillna(method='ffill', axis=1)
        df_data[0:4] = df_data.iloc[0:4].fillna('')
        df_data.columns = df_data.iloc[0:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
        df_data['Region'].replace('', np.nan, inplace=True)
        df_data.dropna(subset=['Region'], inplace=True)
        df_data = df_data[1:]
        try:
            df_data = df_data.loc[:, ~df_data.columns.str.contains('^Unnamed')]
        except:
            pass
        file_name = f'{sheets[k]} - Estimates of people who inject drugs living with HIV HCV HBV'
        df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
        try:
            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
            BodyDict["JsonDetails"]["table"] = file_name
            BodyDict["JsonDetails"]["description"] = note + '. ' + defin_pdf
            BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
            BodyDict["JsonDetails"]["topic"] = topics_name[3]
            BodyDict["JsonDetails"]["created"] =  last_update_pdf
            BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
            BodyDict["JsonDetails"]["concept"] = concept_pdf
            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source_pdf
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

    df_5 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\4.1_-_Estimates_of_people_who_inject_drugs_living_with_HIV_HCV_HBV.xlsx',sheets[5])
    df_5 = df_5.dropna(axis='columns', how='all')
    df_5.dropna(axis=0, how='any', inplace=False)
    df_5[0:3] = df_5.iloc[0:3].fillna(method='ffill', axis=1)
    df_5[0:3] = df_5.iloc[0:3].fillna('')
    df_5.columns = df_5.iloc[0:3].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    notes_5 = df_5.iloc[26,1]
    df_5=df_5[3:25]
    df_5[df_5.columns[0]] = df_5[df_5.columns[0]].ffill(axis = 0)
    df_5[df_5.columns[1]] = df_5[df_5.columns[1]].ffill(axis = 0)
    file_name = f'{sheets[5]} - Estimates of people who inject drugs living with HIV HCV HBV'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes_5 + '. ' + defin_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topics_name[3]
        BodyDict["JsonDetails"]["created"] =  last_update_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_pdf
        BodyDict["JsonDetails"]["concept"] = concept_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source_pdf
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

    notes = []
    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\5.1_-_Treatment_by_primary_drug_use_2020_data.xlsx')
    df[3:6] = df.iloc[3:6].fillna(method='ffill', axis=1)
    df[3:6] = df.iloc[3:6].fillna('')
    df.columns = df.iloc[3:6].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    source = df.iloc[67,2] + '. ' + df.iloc[68,2] + '. ' + df.iloc[69,2]
    df_notes = df.iloc[73:76,2:4]
    for i in range(len(df_notes.index)):
        f = df_notes.iloc[i].to_list()
        f = [' : '.join(map(str,f))]
        notes.append(f[0])
    note = [' ,'.join(map(str,notes))][0]
    df = df.iloc[6:65]
    file_name = f'Treatment by primary drug use 2020 data'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_1
        BodyDict["JsonDetails"]["topic"] = topics_name[4]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] =  source + '. ' + source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\5.2_-_Treatment_by_primary_drug_use_2014-2019.xlsx')
    note_1 = df.iloc[4,0]
    df[6:9] = df.iloc[6:9].fillna(method='ffill', axis=1)
    df[6:9] = df.iloc[6:9].fillna('')
    df.columns = df.iloc[6:9].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
    df = df[9:625]
    df_notes = df.iloc[627:649,0:2]
    for i in range(len(df_notes.index)):
        f = df_notes.iloc[i].to_list()
        f = [' : '.join(map(str,f))]
        notes.append(f[0])
    note = note_1 + [' ,'.join(map(str,notes))][0]
    df = df[6:625]
    file_name = f'Treatment by primary drug use 2014-2019'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[4]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.1_-_Illicit_coca_bush_cultivation.xlsx')
    df = df.dropna(how='all', axis=1)
    note = df.iloc[1,0]
    df.columns = df.iloc[3,:]
    df.rename(columns = {df.columns[0]:'country'}, inplace = True)
    notes = note + '. ' + df.iloc[12,0] + '. ' + df.iloc[13,0] + '. ' + df.iloc[14,0] + '. ' + df.iloc[15,0]
    sources = df.iloc[11,0].replace('\n','')
    df = df[5:10]
    file_name = f'Illicit coca bush cultivation'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = sources+ '. ' +source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.2_-_Eradication_of_coca_bush.xlsx')
    df = df.dropna(how='all', axis=1)
    note = df.iloc[1,0]
    df.columns = df.iloc[3,:]
    df.rename(columns = {df.columns[0]:'country'}, inplace = True)
    sources = df.iloc[12,0].replace('\n','').split('Note:')[0]
    notes = note + '. ' + df.iloc[12,0].replace('\n','').split('Note:')[1]
    df = df[4:10]
    df[df.columns[0]] = df[df.columns[0]].ffill(axis = 0)
    file_name = f'Eradication of coca bush'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = sources+ '. ' +source_2_pdf
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.3_-_Cocaine_manufacture.xlsx')
    sheets = xls.sheet_names
    df_1_3 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.3_-_Cocaine_manufacture.xlsx',sheets[0])
    df_1_3 = df_1_3.dropna(how='all', axis=1)
    top = df_1_3.iloc[2,1]
    df_1_3.columns = df_1_3.iloc[3,:]
    df_1_3.rename(columns = {df_1_3.columns[0]:'country'}, inplace = True)
    source = df_1_3.iloc[9,0]
    notes = df_1_3.iloc[11:19,0].to_list()
    notes = notes[0].replace('\n',' ')
    df_1_3 = df_1_3[4:8]
    file_name = 'Cocaine manufacture'
    df_1_3.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = sources+ '. ' +source_2_pdf
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


    df_publ = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.3_-_Cocaine_manufacture.xlsx',sheets[3])
    df_publ = df_publ.dropna(how='all', axis=1)
    top = df_publ.iloc[2,1]
    df_publ.columns = df_publ.iloc[3,:]
    df_publ.rename(columns = {df_publ.columns[0]:'country'}, inplace = True)
    sources = df_publ.iloc[9,0]
    notes = df_publ.iloc[11:19,0].to_list()
    notes = notes[0].replace('\n','')
    df_publ = df_publ[4:8]
    file_name = f'{sheets[3]} - Cocaine manufacture'
    df_publ.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = sources+ '. ' +source_2_pdf
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

    df_file = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.1.3_-_Cocaine_manufacture.xlsx',sheets[4])
    df_file.columns = df_file.iloc[0,:]
    df_file.rename(columns={df_file.columns[0]: "country"}, inplace = True)
    df_file = df_file[1:]
    df_file.columns = ['country', 2008.0, 2009.0, 2010.0, 2011.0, 2012.0, 2013.0, 2014.0,2015.0, 2016.0, 2017.0, 2018.0]
    file_name = f'{sheets[4]} - Cocaine manufacture'
    df_file.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.2.1_-_Illicit_opium_poppy_cultivation.xlsx')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df[df.columns[0]] = df[df.columns[0]].ffill(axis = 0)
    df.columns = df.iloc[1,:]
    df.rename(columns={df.columns[0]: "country"}, inplace = True)
    source = df.iloc[25,0].replace('\n','').split('Note: ')[0]
    note = df.iloc[25,0].replace('\n','').split('Note: ')[1]
    df=df[2:24]
    file_name = f'Illicit opium poppy cultivation'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source + '. ' + source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.2.2_-_Potential_production_of_oven-dry_opium.xlsx')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df[df.columns[0]] = df[df.columns[0]].ffill(axis = 0)
    df.columns = df.iloc[0,:]
    df.rename(columns={df.columns[0]: "country"}, inplace = True)
    df.rename(columns={df.columns[8]: "a"}, inplace = True)
    df = df.drop('a', axis=1)
    source = df.iloc[23,0].replace('\n','')
    note = df.iloc[24,0].replace('\n','')
    df=df[1:23]
    file_name = f'Potential production of oven-dry opium'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source + '. ' + source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.2.3_-_Eradication_of_opium_poppy_and_cultivation_of_opium_poppy_and_production.xlsx')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df.columns = df.iloc[0,:]
    df = df.drop(index=df.iloc[7].name)
    df = df[1:]
    file_name = f'Eradication of opium poppy and cultivation of opium poppy and production'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.2.4_-_Heroin_manufacture.xlsx')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df.columns = df.iloc[1,:]
    df.rename(columns={df.columns[0]: "country"}, inplace = True)
    notes = df.iloc[7,0]
    df = df[2:6]
    file_name = f'Heroin manufacture'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = notes +  '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\6.3.1_-_Cannabis_cultivation_production_and_eradication.xls')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df.columns = df.iloc[1,:]
    df = df.iloc[2:,1:]
    source = df.iloc[369,1]
    note = df.iloc[370,1] + ' ' + df.iloc[371,1]
    df = df[:364]
    file_name = f'Cannabis cultivation production and eradication'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note +  '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[5]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source + '. ' + source_2_pdf
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

    df = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\7.1_-_Drug_seizures_2016-2020.xlsx')
    df.columns = df.iloc[2,:]
    df = df[3:]
    file_name = f'Drug seizures 2016-2020'
    df.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[6]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.1_-_Prices_and_purities_of_drugs.xlsx')
    sheets = xls.sheet_names
    df_8_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.1_-_Prices_and_purities_of_drugs.xlsx',sheets[0])
    file_name = f'{sheets[0]} - Prices and purities of drugs'
    df_8_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df_8_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.1_-_Prices_and_purities_of_drugs.xlsx',sheets[1])
    file_name = f'{sheets[1]} - Prices and purities of drugs'
    df_8_2.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx')
    sheets = xls.sheet_names
    df_1_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[0])
    title_1 = df_1_1.iloc[0,0]
    note_1 = df_1_1.iloc[2,0]
    df_1_1.columns = df_1_1.iloc[4,:]
    df_1_1.rename(columns={df_1_1.columns[0]: "country"}, inplace = True)
    source_1 = df_1_1.iloc[26,0]
    df_1_1 = df_1_1[5:26]
    df_1_1.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]

    file_name = f'{title_1} - Price time series in Western Europe and United States'
    df_1_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_1 +'. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_1 +'. '+ source_2_pdf
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

    df_1_2 =  pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[0])
    title_2 = df_1_2.iloc[28,0]
    df_1_2.columns = df_1_2.iloc[30,:]
    df_1_2.rename(columns={df_1_2.columns[0]: "country"}, inplace = True)
    df_1_2.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    source_2 = df_1_2.iloc[52,0]
    note = df_1_2.iloc[53,0]
    df_1_2=df_1_2[31:52]

    file_name = f'{title_2} - Price time series in Western Europe and United States'
    df_1_2.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2 + '. ' + source_2_pdf
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

    df_2_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[1])
    title_2_1 = df_2_1.columns[0]
    note_2_1 = df_2_1.iloc[1,0]
    df_2_1.columns = df_2_1.iloc[3,:]
    df_2_1.rename(columns={df_2_1.columns[0]: "country"}, inplace = True)
    df_2_1.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    source_2_1 = df_2_1.iloc[26,0]
    df_2_1=df_2_1[4:25]
    file_name = f'{title_2_1} - Price time series in Western Europe and United States'
    df_2_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_2_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_1 + '. ' + source_2_pdf
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

    df_2_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[1])
    title_2_2 = df_2_2.iloc[30,0]
    note_2_2 = df_2_2.iloc[55,0]
    source_2_2 =  df_2_2.iloc[54,0]
    df_2_2.columns = df_2_2.iloc[32,:]
    df_2_2.rename(columns={df_2_2.columns[0]: "country"}, inplace = True)
    df_2_2.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    df_2_2 = df_2_2[33:54]

    file_name = f'{title_2_2} - Price time series in Western Europe and United States'
    df_2_2.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_2_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_1 + '. ' + source_2_pdf
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

    df_3_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[2])
    title_3_1 = df_3_1.columns[0]
    note_3_1 = df_3_1.iloc[6,0].split('\n**')[0]
    source_3_1 = df_3_1.iloc[6,0].split('\n**')[1]
    df_3_1.columns = df_3_1.iloc[1,:]
    df_3_1.rename(columns={df_3_1.columns[0]: "country"}, inplace = True)
    df_3_1.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    df_3_1 = df_3_1[2:6]
    file_name = f'{title_3_1} - Price time series in Western Europe and United States'
    df_3_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_3_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topic_name_7
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_3_1 + '. ' + source_2_pdf
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

    df_3_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[2])
    title_3_2 = df_3_2.iloc[8,0]
    source_3_2 = df_3_2.iloc[13,0]
    df_3_2.columns = df_3_2.iloc[10,:]
    df_3_2.rename(columns={df_3_2.columns[0]: "country"}, inplace = True)
    df_3_2.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    df_3_2 = df_3_2[11:13]
    file_name = f'{title_3_2} - Price time series in Western Europe and United States'
    df_3_2 .to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_3_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topic_name_8
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_3_2 + '. ' + source_2_pdf
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

    df_4_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[3])
    title_4_1 = df_4_1.columns[0]
    note_4_1 = df_4_1.iloc[6,0].split('\n**')[0]
    source_4_1 = df_4_1.iloc[6,0].split('\n**')[1]
    df_4_1.columns = df_4_1.iloc[1,:]
    df_4_1.rename(columns={df_3_1.columns[0]: "country"}, inplace = True)
    df_4_1.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    df_4_1 = df_4_1[2:6]
    file_name = f'{title_4_1} - Price time series in Western Europe and United States'
    df_4_1 .to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_4_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topic_name_8
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_4_1 + '. ' + source_2_pdf
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

    df_4_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\8.3_-_Price_time_series_in_Western_Europe_and_United_States.xlsx',sheets[3])
    title_4_2 = df_4_2.iloc[8,0]
    source_4_2 = df_4_2.iloc[13,0]
    df_4_2.columns = df_4_2.iloc[10,:]
    df_4_2.rename(columns={df_4_2.columns[0]: "country"}, inplace = True)
    df_4_2.columns = [   "country", 1990.0, 1991.0, 1992.0, 1993.0, 1994.0, 1995.0, 1996.0,
                1997.0, 1998.0, 1999.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0,
                2005.0, 2006.0, 2007.0, 2008.0, 2009.0, 2010.0, 2011.0, 2012.0,
                2013.0, 2014.0, 2015.0, 2016.0, 2017.0, 2018.0, 2019.0, 2020.0]
    df_4_2 = df_4_2[11:13]
    file_name = f'{title_4_2} - Price time series in Western Europe and United States'
    df_4_2 .to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = note_4_1 + '. ' + defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_4_2 + '. ' + source_2_pdf
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


    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/9.1_-_Clandestine_laboratories_detected_and_dismantled.xlsx"
        BodyDict["JsonDetails"]["table"] = 'Clandestine laboratories detected and dismantled'
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[6]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    xls = pd.ExcelFile('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\10.1_-_Drug_related_crimes.xlsx')
    sheets = xls.sheet_names
    df_10_1 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\10.1_-_Drug_related_crimes.xlsx',sheets[0])
    file_name = f'{sheets[0]} - Drug related crimes'
    df_10_1.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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

    df_10_2 = pd.read_excel('\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\10.1_-_Drug_related_crimes.xlsx',sheets[1])
    file_name = f'{sheets[1]} - Drug related crimes'
    df_10_2.to_csv(f'\\\\10.30.31.77\\data_collection_dump\\RawData\\UNODC_1\\{file_name}.csv',index=False, encoding="utf-8")
    try:
        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNODC_1/{file_name}.csv"
        BodyDict["JsonDetails"]["table"] = file_name
        BodyDict["JsonDetails"]["description"] = defin_2_pdf
        BodyDict["JsonDetails"]["tags"][0]["name"] = tag_2
        BodyDict["JsonDetails"]["topic"] = topics_name[7]
        BodyDict["JsonDetails"]["created"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["last_modified"] =  last_update_2_pdf
        BodyDict["JsonDetails"]["concept"] = concept_2_pdf
        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source_2_pdf
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
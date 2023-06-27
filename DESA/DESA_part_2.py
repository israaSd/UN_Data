import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import urllib.request
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
import shutil
import time
import os
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI


def execute():
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNDESA_part_2_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_2' # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()
    base_path_2 = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_2'
    base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_2'
    import timer
    link_tag = []
    topic_list = []
    driver.get("https://www.un.org/development/desa/pd/data-landing-page")
    parent_node = driver.find_element(By.CLASS_NAME,'pane-content')
    topics = parent_node.find_elements(By.TAG_NAME,'li')
    for t in topics:
        topic= t.text
        topic_list.append(topic)
        link = t.find_element(By.TAG_NAME,'a').get_attribute('href')#['href']
        link_tag.append(link)
    driver.get(link_tag[1])
    base_path_2 = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_2'
    topic = topic_list[1]
    parent_concept = driver.find_element(By.ID,'form1')
    concept = parent_concept.find_element(By.TAG_NAME,'p').text
    download_data_files = driver.find_element(By.CLASS_NAME,'TileLine')       
    link_data_files = download_data_files.find_element(By.TAG_NAME,'a').get_attribute('href')
    driver.get(link_data_files) 
    page_source = driver.page_source        
    soup = BeautifulSoup(page_source, 'html.parser')

    top = soup.find(class_="TabsAsButtons")
    # ul = top.find('ul')
    # all_li = ul.find_all('li')
    # for li in all_li:
    #     tag_name = li.text
    #     tags.append(tag_name)
    div_tables = top.find_all('div')
    for div_table in div_tables:
        tag_name = top.find_all('li')[div_tables.index(div_table)].text
        table_content = div_table.find('table')#(class_="filestable")
        # print(table_content)
        rows = table_content.find_all('tr')
        for row in rows[1:]:
            cells = row.find_all('td')
            cells_text = [cell.text for cell in cells]
            for cell in cells:
                links_files = cell.find_all('a')
                for link_f in links_files:
                    link_file = link_data_files + link_f['href']
                    title = cells_text[1].replace(':','')
                    # print(title)
                    
                    
                    BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}", ## Point to downloaded data for conversion
                        "JsonDetails":{
                            ## Required
                            "organisation": "un-agencies",
                            "source": "DESA",
                            "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
                            "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
                            "table" : title,
                            "description" : title,
                            ## Optional
                            "JobType": "JSON",
                            "CleanPush": True,
                            "Server": "str",
                            "UseJsonFormatForSQL":  False,
                            "CleanReplace":True,
                            "MergeSchema": False,
                            "tags": [{
                            "name": tag_name
                            }],
                            "limitations":"",
                            "concept":  concept,
                            "periodicity":  "",
                            "topic":  topic,
                            "created":"",                       ## this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                            "last_modified": "",                ## ""               ""                  ""              ""
                            "TriggerTalend" :  False, ## initialise to True for production
                            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_2" # initialise as empty string for production.
                        }
                    }
                        

                    if link_file.endswith('xls'):
                        urllib.request.urlretrieve(link_file,f"{base_path}\\{title}.xls")
                        if title.startswith('Data'):
                            df = pd.read_excel(f"{base_path}\\{title}.xls")
                            
                            df.columns = df.iloc[15].to_list()
                            df[16:].to_excel(f"{base_path}\\{title}.xlsx",index = False)
                            try:
                                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                                BodyDict["JsonDetails"]["table"] = title
                                BodyDict["JsonDetails"]["description"] = title
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
                            
                            note = []
                            xls = pd.ExcelFile(f"{base_path}\\{title}.xls")
                            sheets = xls.sheet_names
                            df_2 = pd.read_excel(f"{base_path}\\{title}.xls",sheets[1])
                            for i in range(len(df_2.index)):
                                f = df_2.iloc[i].to_list()
                                f = [' : '.join(map(str,f))]
                                note.append(f[0])
                            notes = [' , '.join(map(str,note))]
                            notes = 'Notes, ' + notes[0]
                            df = pd.read_excel(f"{base_path}\\{title}.xls",sheets[0])
                            df.columns = df.iloc[15].to_list()
                            df = df[16:]
                            
                            df.to_excel(f"{base_path}\\{title}.xlsx",index = False)
                            try:
                                BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                                BodyDict["JsonDetails"]["table"] = title
                                BodyDict["JsonDetails"]["description"] = notes
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
                            # print(notes)
                    if link_file.endswith('xlsx'):
                        urllib.request.urlretrieve(link_file,f"{base_path}\\{title}.xlsx")
                        note = []
                        xls = pd.ExcelFile(f"{base_path}\\{title}.xlsx")
                        sheets = xls.sheet_names
                        df = pd.read_excel(f"{base_path}\\{title}.xlsx",sheets[0])
                        df.iloc[14:16] = df.iloc[14:16].fillna(method='ffill', axis=1)
                        df.iloc[14:16] = df.iloc[14:16].fillna('')
                        df.columns = df.iloc[14:16].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                        df = df[16:]
                        df_2 = pd.read_excel(f"{base_path}\\{title}.xlsx",sheets[1])
                        for i in range(len(df_2.index)):
                            f = df_2.iloc[i].to_list()
                            f = [' : '.join(map(str,f))]
                            note.append(f[0])
                        notes = [' , '.join(map(str,note))]
                        notes = 'Notes, ' + notes[0]
                        df.to_excel(f"{base_path}\\{title}.xlsx",index = False)
                        try:
                            BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                            BodyDict["JsonDetails"]["table"] = title
                            BodyDict["JsonDetails"]["description"] = notes
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
                        # print(notes)

                    if link_file.endswith('zip'):
                        urllib.request.urlretrieve(link_file,f"{base_path}\\{title}.zip")
                        # print(title)
                        # print(link_file)
                        from zipfile import ZipFile

                        base_path_2 = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_2'
                        from zipfile import ZipFile
                        import shutil
                        for zipfiles in os.listdir(base_path):
                            if zipfiles.endswith(".zip"):
                                if zipfiles.startswith('20'):
                                    shutil.unpack_archive(fr"{base_path}\{zipfiles}", base_path)
                                    os.remove(fr"{base_path}\{zipfiles}")
                                if zipfiles.startswith('199'):
                                    filename = zipfiles.split(".zip")[0]
                                    year = filename.split(' ')[0]
                                    shutil.unpack_archive(fr"{base_path}\{zipfiles}", base_path)
                                    
                                    folder_1 = fr'{base_path}\Urban-Agglomerations'
                                    folder_2 = fr'{base_path}\Urban-Rural-Areas'
                                    os.rename(folder_1,fr'{base_path}\{year} Urban-Agglomerations')
                                    os.rename(folder_2,fr'{base_path}\{year} Urban-Rural-Areas')
                                    
                                    name_1 = ZipFile(fr'{base_path}\\{zipfiles}').namelist()
                                    for files in os.listdir(fr'{base_path}\{year} Urban-Agglomerations'):
                                        shutil.move(fr'{base_path}\{year} Urban-Agglomerations\{files}',fr'{base_path}\{files}')
                                        os.rename(fr'{base_path}\{files}',fr'{base_path}\{year} Urban-Agglomerations - {files}')
                                    for files in os.listdir(fr'{base_path}\{year} Urban-Rural-Areas'):
                                        shutil.move(fr'{base_path}\{year} Urban-Rural-Areas\{files}',fr'{base_path}\{files}')
                                        os.rename(fr'{base_path}\{files}',fr'{base_path}\{year} Urban-Rural-Areas - {files}')
                                    os.remove(fr"{base_path}\{zipfiles}")

                    
    BodyDict = {
        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}", ## Point to downloaded data for conversion
        "JsonDetails":{
            ## Required
            "organisation": "un-agencies",
            "source": "DESA",
            "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
            "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
            "table" : title,
            "description" : title,
            ## Optional
            "JobType": "JSON",
            "CleanPush": True,
            "Server": "str",
            "UseJsonFormatForSQL":  False,
            "CleanReplace":True,
            "MergeSchema": False,
            "tags": [{
            "name": 'Archive'
            }],
            "limitations":"",
            "concept":  concept,
            "periodicity":  "",
            "topic":  topic,
            "created":"",                       ## this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
            "last_modified": "",                ## ""               ""                  ""              ""
            "TriggerTalend" :  False, ## initialise to True for production
            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_2" # initialise as empty string for production.
        }
    }
        

    for file in os.listdir(base_path):
        if file.startswith('1992'):
            if file.endswith('XLS'):
                if file.startswith('1992 Urban-'):
                    try:
                        tit = file.split('.')[0]
                        new_file = file.replace('XLS','xls')
                        os.rename(f'{base_path}\\{file}',f'{base_path}\\{new_file}')
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{new_file}"
                        BodyDict["JsonDetails"]["table"] = tit
                        BodyDict["JsonDetails"]["description"] = tit
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
        elif file.startswith('1994'):
            if file.endswith('XLS'):
                if file.startswith('1994 Urban-'):
                    try:
                        tit = file.split('.')[0]
                        new_file = file.replace('XLS','xls')
                        os.rename(f'{base_path}\\{file}',f'{base_path}\\{new_file}')
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{new_file}"
                        BodyDict["JsonDetails"]["table"] = tit
                        BodyDict["JsonDetails"]["description"] = tit
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
        elif file.startswith('1999'):

            if file.endswith('xls'):
                print(file)
                if file=='1999 Urban-Agglomerations - WUP1999F10-Cities_Over_750K.xls':
                    file = fr'{base_path}\1999 Urban-Agglomerations - WUP1999F10-Cities_Over_750K.xls'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[8,0] + ' ' + data.iloc[9,0]
                    title = '1999'+ ' ' + title.replace(':',' ')
                    data.columns = data.iloc[13].to_list()
                    data = data[14:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))][0]
                    data.to_excel(fr'{base_path}\{title}.xlsx',index=False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif 'DESCRIPTION' in file:
                    print(file)
                else:
                    import pandas as pd
                    notes = []
                    file = fr'{base_path}\{file}'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[8,0] 
                    title = '1999' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[12].to_list()
                    data = data[13:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))][0]
                    data.to_excel(fr'{base_path}\{title}.xlsx',index=False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                                

                            
    for folders in os.listdir(base_path):
        if folders.startswith("WUP20"):
    #         print(folders)
            if 'DOCUMENTATION' in folders :
                print(folders)
            else :
                for files in os.listdir(f'{base_path}\\{folders}'):
                    shutil.move(fr'{base_path}\{folders}\{files}',fr'{base_path}\{files}') 
                    # print(files)      
                    # 
    BodyDict = {
        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}", ## Point to downloaded data for conversion
        "JsonDetails":{
            ## Required
            "organisation": "un-agencies",
            "source": "DESA",
            "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
            "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
            "table" : title,
            "description" : title,
            ## Optional
            "JobType": "JSON",
            "CleanPush": True,
            "Server": "str",
            "UseJsonFormatForSQL":  False,
            "CleanReplace":True,
            "MergeSchema": False,
            "tags": [{
            "name": 'Archive'
            }],
            "additional_data_sources": [{
                "name": "",
                "url" : ""
            }],
            "limitations":"",
            "concept":  concept,
            "periodicity":  "",
            "topic":  topic,
            "created":"",                       ## this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
            "last_modified": "",                ## ""               ""                  ""              ""
            "TriggerTalend" :  False, ## initialise to True for production
            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_2" # initialise as empty string for production.
        }
    }   

    for excel_f in os.listdir(base_path):
        if excel_f.endswith('xls'):
            if excel_f.startswith('WUP2009'):
                if excel_f=='WUP2009-F01-Total_Urban_Rural.xls':
                    file = f'{base_path}\\WUP2009-F01-Total_Urban_Rural.xls'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[5,2]
                    title = '2009'+ ' ' + title.replace(':','')
                    df.iloc[8:10] = df.iloc[8:10].fillna(method='ffill', axis=1)
                    df.iloc[8:10] = df.iloc[8:10].fillna('')
                    df.columns = df.iloc[8:10].apply(lambda x: ' \n '.join(map(str,[y for y in x if y])), axis=0)
                    df.rename(columns={df.columns[-1]: df.columns[-1].replace(' \n Total','')},inplace=True)
                    # df[10:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    df = df[10:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"{base_path}\\{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif excel_f.startswith('WUP2009-F11'):
                    file = f'{base_path}//{excel_f}'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[5,0]
                    title = '2009'+ ' ' + title.replace(':','')
                    df.columns = df.iloc[9].to_list()
                    df.columns
                    # df[10:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    df = df[10:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif excel_f.startswith('WUP2009-F17'):
                    file = f'{base_path}//{excel_f}'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[5,2]
                    title = '2009'+ ' ' + title.replace(':','')
                    df.columns = df.iloc[9].to_list()
                    df.columns
                    # df[10:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    df = df[10:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                else:
                    file = f'{base_path}//{excel_f}'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[5,3]
                    title = '2009'+ ' ' + title.replace(':','')
                    df.columns = df.iloc[9].to_list()
                    df.columns
                    # df[10:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    df = df[10:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
            elif excel_f.startswith('WUP2011'):
                if excel_f=='WUP2011-F01-Total_Urban_Rural.xls':
                    file = f'{base_path}\\WUP2011-F01-Total_Urban_Rural.xls'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,3] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.iloc[11:13] = data.iloc[11:13].fillna(method='ffill', axis=1)
                    data.iloc[11:13] = data.iloc[11:13].fillna('')
                    data.columns = data.iloc[11:13].apply(lambda x: ' \n '.join(map(str,[y for y in x if y])), axis=0)
                    data = data[13:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif excel_f.startswith('WUP2011-F11'):
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,0] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[11].to_list()
                    data = data[12:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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

                elif excel_f.startswith('WUP2011-F14'):
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,4] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[11].to_list()
                    data = data[12:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif excel_f.startswith('WUP2011-F15'):
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,4] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[11].to_list()
                    data = data[12:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                elif excel_f.startswith('WUP2011-F16'):
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,4] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[11].to_list()
                    data = data[12:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                else:
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []

                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    data = pd.read_excel(file,sheets[0])
                    title = data.iloc[6,3] 
                    title = '2011' + ' ' + title.replace(':',' ')
                    data.columns = data.iloc[11].to_list()
                    data = data[12:]
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    source = note.split('\n Source:')[1]
                    data.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                
            elif excel_f.startswith('WUP2014'):
                if excel_f=='WUP2014-F00-Locations.xls':
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[8,2]
                    title = '2014' + ' ' + title.replace(':',' ')
                    
                    df.columns = df.iloc[15].to_list()
                    # df[16:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    df = df[16:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
                    file = f'{base_path}\\{excel_f}'
                    import pandas as pd
                    notes = []
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df = pd.read_excel(file,sheets[0])
                    title = df.iloc[8,0]
                    title = '2014' + ' ' + title.replace(':',' ')
                    
                    df.columns = df.iloc[15].to_list()
                    # df[16:].to_excel('test_file_title_2.xlsx',index = False)
                    data_note = pd.read_excel(file,sheets[1])
                    for i in range(len(data_note.index)):
                        f = data_note.iloc[i].to_list()
                        notes.append(f[0])
                    note = [' ,\n '.join(map(str,notes))]
                    note = 'Notes, ' + note[0]
                    df = df[16:]
                    df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = note
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
        elif excel_f.endswith('XLS'):
            if excel_f.startswith('WPP2011-F00'):
                file = f'{base_path}\\{excel_f}'
                import pandas as pd
                notes = []
                xls = pd.ExcelFile(file)
                sheets = xls.sheet_names
                df = pd.read_excel(file,sheets[0])
                title = df.iloc[6,3]
                title = '2011' + ' ' + title.replace(':','')
                df.iloc[12:14] = df.iloc[12:14].fillna(method='ffill', axis=1)
                df.iloc[12:14] = df.iloc[12:14].fillna('')
                df.columns = df.iloc[12:14].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                data_note = pd.read_excel(file,sheets[1])
                for i in range(len(data_note.index)):
                    f = data_note.iloc[i].to_list()
                    notes.append(f[0])
                note = [' ,\n '.join(map(str,notes))]
                note = 'Notes, ' + note[0]
                df = df[14:]
                df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = note
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
            elif excel_f.startswith('WPP2009-F00'):
                file = f'{base_path}\\{excel_f}'
                import pandas as pd
                notes = []
                xls = pd.ExcelFile(file)
                sheets = xls.sheet_names
                df = pd.read_excel(file,sheets[0])
                title = df.iloc[6,3]
                title = '2009' + ' ' + title.replace(':','')
                df.iloc[12:14] = df.iloc[12:14].fillna(method='ffill', axis=1)
                df.iloc[12:14] = df.iloc[12:14].fillna('')
                df.columns = df.iloc[12:14].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                data_note = pd.read_excel(file,sheets[1])
                for i in range(len(data_note.index)):
                    f = data_note.iloc[i].to_list()
                    notes.append(f[0])
                note = [' ,\n '.join(map(str,notes))]
                note = 'Notes, ' + note[0]
                source = note.split('\n Source:')[1]
                df = df[14:]
                df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = note
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

execute()
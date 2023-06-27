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
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNDESA_part_3_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = f"\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_3" # local, gets current working directory
    base_path
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    link_tag = []
    base_path_test = base_path
    base_path_test = '\\\\10.30.31.77\\data_collection_dump\\RawData\\DESA_part_3'
    driver.get("https://www.un.org/development/desa/pd/data-landing-page")
    parent_node = driver.find_element(By.CLASS_NAME,'pane-content')
    topics = parent_node.find_elements(By.TAG_NAME,'li')
    for t in topics:
        # topic = t.text
        # print(tag)
        # link = t.find_element(By.TAG_NAME,'a').get_attribute('href')#['href']
        # link_tag.append(link)
        BodyDict = {
            "JobPath":"", #* Point to downloaded data for conversion #
            "JsonDetails":{
                    ## Required
                    "organisation": "un-agencies",
                    "source": "DESA",
                    "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
                    "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
                    "table" : '',
                    "description" : '', #+ indc + '\n' + note,
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
                        "name": ""
                    }],
                    "limitations":"",
                    "concept":  '',
                    "periodicity":  "",
                    "topic":  '',
                    "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                    "last_modified": "",                #* ""               ""                  ""              ""
                    "TriggerTalend" :  False,    #* initialise to True for production
                    "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_3" #* initialise as empty string for production.
                }
            }
        
        if t.text=='World Population Prospects':
            topic = t.text
            print(topic)
            # link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            # driver.get(link)
            # page_source = driver.page_source
            # driver.back()
        elif t.text=='World Urbanization Prospects':
            topic = t.text
            print(topic)
            # link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            # driver.get(link)
            # page_source = driver.page_source
            # driver.back()
        elif t.text == 'International Migrant Stock':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                import pandas as pd
                note = []
                if 'Notes' in title_f :
                    file = f'{base_path}\\International Migrant Stock International Migrant Stock 2020 Notes.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    for sheet in sheets:
                        notes = ""
                        if sheet=='Table of contents':
                            print(sheet)    
                        elif sheet=='Notes':
                            df_note = pd.read_excel(file,sheet)
                            df_note = df_note.iloc[10:,0:2]
                            for i in range(len(df_note.index)):
                                f = df_note.iloc[i].to_list()
                                f = [' : '.join(map(str,f))]
                                note.append(f[0])
                            notes = [' , '.join(map(str,note))]
                            notes = 'Notes, ' + notes[0]
                        else:
                            df_data = pd.read_excel(file,sheet)
                            title = df_data.iloc[1,5].replace(':','')
                            df_data.iloc[8:10] = df_data.iloc[8:10].fillna(method='ffill', axis=1)
                            df_data.iloc[8:10] = df_data.iloc[8:10].fillna('')
                            df_data.columns = df_data.iloc[8:10].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                            df_data = df_data.iloc[10:,1:] 
                            df_data.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                        try :    
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                            BodyDict["JsonDetails"]["table"] = title
                            if notes == "":
                                notes = title
                            BodyDict["JsonDetails"]["description"] = notes    
                            BodyDict["JsonDetails"]["concept"] = concept    
                            BodyDict["JsonDetails"]["topic"] = start_title
                            BodyDict["JsonDetails"]["tags"][0]["name"] = topic
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
                elif ' Total, destination' in title_f : 
                    file = f'{base_path_test}\\International Migrant Stock International Migrant Stock 2020 Total, destination.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    for sheet in sheets:
                        notes = ""
                        if sheet=='Table of contents':
                            print(sheet)    
                        elif sheet=='Notes':
                            df_note = pd.read_excel(file,sheet)
                            df_note = df_note.iloc[10:,1]
                            for i in range(len(df_note.index)):
                                f = df_note.iloc[i]#.to_list()
                                # f = [' : '.join(map(str,f))]
                                note.append(f)
                            notes = [' , '.join(map(str,note))]
                            notes = 'Notes, ' + notes[0]
                        else:
                            df_data = pd.read_excel(file,sheet)
                            title = a.text + ' ' + df_data.iloc[1,4].replace(':','')
                            df_data.iloc[6:10] = df_data.iloc[6:10].fillna(method='ffill', axis=1)
                            df_data.iloc[6:10] = df_data.iloc[6:10].fillna('')
                            df_data.columns = df_data.iloc[6:10].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                            df_data = df_data[10:] 
                            df_data.to_excel(f'{base_path_test}\\{title}.xlsx',index=False)
                            try :
                                BodyDict["JobPath"] = f'{base_path_test}\\{title}.xlsx'
                                BodyDict["JsonDetails"]["table"] = title
                                if notes == "":
                                    notes = title
                                BodyDict["JsonDetails"]["description"] = notes
                                BodyDict["JsonDetails"]["concept"] = concept
                                BodyDict["JsonDetails"]["topic"] = start_title
                                BodyDict["JsonDetails"]["tags"][0]["name"] = topic
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
                    file = f'{base_path}\\{title_f}.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    for sheet in sheets:
                        notes = ""
                        if sheet=='Table of contents':
                            print(sheet)    
                        elif sheet=='Notes':
                            df_note = pd.read_excel(file,sheet)
                            df_note = df_note.iloc[10:,1]
                            for i in range(len(df_note.index)):
                                f = df_note.iloc[i]#.to_list()
                                # f = [' : '.join(map(str,f))]
                                note.append(f)
                            notes = [' , '.join(map(str,note))]
                            notes = 'Notes, ' + notes[0]
                        else:
                            df_data = pd.read_excel(file,sheet)
                            title = a.text + ' ' + df_data.iloc[1,4].replace(':','')
                            df_data.iloc[8:10] = df_data.iloc[8:10].fillna(method='ffill', axis=1)
                            df_data.iloc[8:10] = df_data.iloc[8:10].fillna('')
                            df_data.columns = df_data.iloc[8:10].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                            df_data = df_data[10:] 
                            df_data.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                            try :
                                BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                                BodyDict["JsonDetails"]["table"] = title
                                if notes == "":
                                    notes = title
                                BodyDict["JsonDetails"]["description"] = notes
                                BodyDict["JsonDetails"]["concept"] = concept
                                BodyDict["JsonDetails"]["topic"] = start_title
                                BodyDict["JsonDetails"]["tags"][0]["name"] = topic
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
        elif t.text == 'International Migration Flows':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                import pandas as pd
                file = f'{base_path}\\{title_f}.xlsx'
                df = pd.read_excel(file)
                title = df.iloc[7,0]
                title = title.replace(':',',')
                df.columns = df.iloc[15,:].to_list()
                df.rename(columns={df.columns[0]: 'Reporting country CntName'},inplace=True)
                df = df[16:]
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                try :
                    BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = title
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = page_header 
                    BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
        elif t.text=='Family Planning Indicators':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('zip'):
                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }

                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.zip', 'wb') as f:
                        f.write(r.content)
                for zipfile in os.listdir(base_path):
                    if zipfile.endswith(".zip"):
                        shutil.unpack_archive(f"{base_path}\{zipfile}", base_path)
                try :
                    BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/Data_FamilyPlanningIndicators_2022.csv'
                    BodyDict["JsonDetails"]["table"] = 'Data_FamilyPlanningIndicators_2022'
                    BodyDict["JsonDetails"]["description"] = 'Data_FamilyPlanningIndicators_2022'
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = page_header 
                    BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
        elif t.text == 'World Contraceptive Use':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                    import pandas as pd
                    info_notes = []
                    descriptions = []
                    file = f'{base_path}\\{title_f}.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df_info_note =  pd.read_excel(file,sheets[0])
                    info_note = df_info_note.iloc[6,1]
                    df_desc = pd.read_excel(file,sheets[1])
                    df_desc = df_desc.iloc[:,1:]
                    for i in range(len(df_desc.index)):
                        f = df_desc.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        descriptions.append(f[0])
                    description = [' ,\n '.join(map(str,descriptions))][0]
                    # for last 2 sheet
                    for sheet in sheets[2:4]:
                        df_data = pd.read_excel(file,sheet)
                        title = df_data.iloc[1,4]
                        title = title.replace('\n',',')
                        df_data.iloc[6:8] = df_data.iloc[6:8].fillna(method='ffill', axis=1)
                        df_data.iloc[6:8] = df_data.iloc[6:8].fillna('')
                        df_data.columns = df_data.iloc[6:8].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                        df_data = df_data[8:]
                        df_data.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                            BodyDict["JsonDetails"]["table"] = title
                            BodyDict["JsonDetails"]["description"] = description + '\n' + info_note
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] = page_header 
                            BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
        elif t.text == 'World Fertility Data':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                    import pandas as pd
                    info_notes = []
                    descriptions = []
                    file = f'{base_path_test}\\{title_f}.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df_info_note =  pd.read_excel(file,sheets[0])
                    info_note = df_info_note.iloc[5,1]

                    df_desc = pd.read_excel(file,sheets[1])
                    df_desc = df_desc.iloc[:,1:]
                    for i in range(len(df_desc.index)):
                        f = df_desc.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        descriptions.append(f[0])
                    description = [' ,\n '.join(map(str,descriptions))][0]
                    df_data = pd.read_excel(file,sheets[2])
                    title = df_data.iloc[1,0]
                    df_data.columns = df_data.iloc[5].to_list()
                    df_data = df_data[6:]
                    df_data.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                    try :
                        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = description + '\n' + info_note
                        BodyDict["JsonDetails"]["concept"] = concept
                        BodyDict["JsonDetails"]["topic"] = page_header 
                        BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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

        elif t.text == 'Household Size and Composition':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title_f = page_header + ' '+ start_title + ' ' + a.text 
                title_f = title_f.replace('\xa0\xa0','')
                link = a['href']
                print(title_f)
                print(link)
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title_f}.xlsx', 'wb') as f:
                        f.write(r.content)
                    import pandas as pd
                    definitions = []
                    sources = []
                    footnotes = []
                    file = f'{base_path}\\{title_f}.xlsx'
                    xls = pd.ExcelFile(file)
                    sheets = xls.sheet_names
                    df_def = pd.read_excel(file,sheets[0])
                    for i in range(len(df_def.index)):
                        f = df_def.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        definitions.append(f[0])
                    definition = [' , '.join(map(str,definitions))]
                    definition = f'{sheets[0]}, ' + definition[0]
                    # definition
                    df_source = pd.read_excel(file,sheets[1])
                    for i in range(len(df_source.index)):
                        f = df_source.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        sources.append(f[0])
                    source = [' ,\n '.join(map(str,sources))]
                    # source
                    df_data = pd.read_excel(file,sheets[2])
                    df_data.iloc[2:4] = df_data.iloc[2:4].fillna(method='ffill', axis=1)
                    df_data.iloc[2:4] = df_data.iloc[2:4].fillna('')
                    df_data.columns = df_data.iloc[2:4].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                    df_data = df_data[4:]
                    df_data.rename(columns={df_data.columns[9]: df_data.columns[9].replace('Households by size (percentage )   ','')},inplace=True)
                    df_data.to_excel(f'{base_path}\\HOUSEHOLD SIZE AND COMPOSITION 2022.xlsx',index=False)
                    df_footnote = pd.read_excel(file,sheets[3])
                    for i in range(len(df_footnote.index)):
                        f = df_footnote.iloc[i].to_list()
                        f = [' : '.join(map(str,f))]
                        footnotes.append(f[0])
                    footnote = [' , '.join(map(str,footnotes))]
                    footnote = f'{sheets[3]}, ' + footnote[0]
                    try :
                        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/HOUSEHOLD SIZE AND COMPOSITION 2022.xlsx'
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = definition + '\n' + footnote
                        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                        BodyDict["JsonDetails"]["concept"] = concept
                        BodyDict["JsonDetails"]["topic"] = page_header 
                        BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
        elif t.text=='Living Arrangements of Older Persons':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            titles = []
            links = []
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            tit_1 = all_li[0].text.split('\n')[0].split('\xa0')[1]
            title_1 = page_header + ' ' + tit_1 + ' '+ all_li[1].text
            title_2 = page_header + ' ' + tit_1 + ' ' + all_li[2].text
            tit_2 = all_li[3].text.split('\n')[0]
            title_3 = page_header + ' ' + tit_2 + ' ' +all_li[4].text
            tit = [title_1,title_2,title_3]
            for li in all_li:
                all_a = part_2_content.find_all('a')
            for a in all_a:
                    link_a = a['href']
                    links.append(link_a)
            for i in range(3):
                title = tit[i]
                link = links[i]
                print(links[i])
                print(tit[i])
                
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }

                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title}.xlsx', 'wb') as f:
                        f.write(r.content)

                    if title == 'Living Arrangements of Older Persons Older persons in collective living quarters  Dataset':
                        import pandas as pd
                        definitions = []
                        sources = []

                        file = f'{base_path}\\{title}.xlsx'
                        xls = pd.ExcelFile(file)
                        sheets = xls.sheet_names

                        df_info_note = pd.read_excel(file,sheets[0])
                        info_note = df_info_note.iloc[3,1]
                        df_def = pd.read_excel(file,sheets[1])
                        for i in range(len(df_def.index)):
                            f = df_def.iloc[i].to_list()
                            f = [' : '.join(map(str,f))]
                            definitions.append(f[0])
                        definition = [' ,\n '.join(map(str,definitions))][0]
                        df_source = pd.read_excel(file,sheets[2])
                        for i in range(len(df_source.index)):
                            f = df_source.iloc[i].to_list()
                            f = [' : '.join(map(str,f))]
                            sources.append(f[0])
                        source = [' ,\n '.join(map(str,sources))][0]

                        df_data = pd.read_excel(file,sheets[3])
                        title_liv = df_data.iloc[1,0].split('\n\n\nCopyright')[0]
                        df_data.iloc[2:5] = df_data.iloc[2:5].fillna(method='ffill', axis=1)
                        df_data.iloc[2:5] = df_data.iloc[2:5].fillna('')
                        df_data.columns = df_data.iloc[2:5].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                        df_data = df_data[5:]
                        df_data.to_excel(f'{base_path}\\{title_liv}.xlsx',index=False)
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title_liv}.xlsx'
                            BodyDict["JsonDetails"]["table"] = title
                            BodyDict["JsonDetails"]["description"] = definition + '\n' + info_note
                            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] = page_header
                            BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
                        import pandas as pd
                        definitions = []
                        sources = []
                        footnotes = []
                        file = f'{base_path}\\{title}.xlsx'
                        xls = pd.ExcelFile(file)
                        sheets = xls.sheet_names

                        df_info_note = pd.read_excel(file,sheets[0])
                        info_note = df_info_note.iloc[3,1]
                        df_def = pd.read_excel(file,sheets[1])
                        for i in range(len(df_def.index)):
                            f = df_def.iloc[i].to_list()
                            f = [' : '.join(map(str,f))]
                            definitions.append(f[0])
                        definition = [' ,\n '.join(map(str,definitions))][0]
                        df_source = pd.read_excel(file,sheets[2])
                        for i in range(len(df_source.index)):
                            f = df_source.iloc[i].to_list()
                            f = [' : '.join(map(str,f))]
                            sources.append(f[0])
                        source = [' ,\n '.join(map(str,sources))][0]
                        df_footnote = pd.read_excel(file,sheets[4])
                        # df_footnote = df_footnote.iloc[:,0]
                        for i in range(len(df_footnote.index)):
                            f = df_footnote.iloc[i].to_list()
                            footnotes.append(f[0])
                        footnote = [' ,\n '.join(map(str,footnotes))][0]
                        df_data = pd.read_excel(file,sheets[3])
                        title_liv = df_data.iloc[1,0].split('\n\n\nCopyright')[0]
                        df_data.iloc[2:4] = df_data.iloc[2:4].fillna(method='ffill', axis=1)
                        df_data.iloc[2:4] = df_data.iloc[2:4].fillna('')
                        df_data.columns = df_data.iloc[2:4].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                        df_data = df_data[4:]
                        df_data.to_excel(f'{base_path}\\{title_liv}.xlsx',index=False)
                        try :
                            BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title_liv}.xlsx'
                            BodyDict["JsonDetails"]["table"] = title
                            BodyDict["JsonDetails"]["description"] = definition + '\n' + info_note + '\n' + footnote
                            BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                            BodyDict["JsonDetails"]["concept"] = concept
                            BodyDict["JsonDetails"]["topic"] = page_header
                            BodyDict["JsonDetails"]["tags"][0]["name"] = topic
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
                

        elif t.text=='Model Life Tables':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title = page_header + start_title + a.text 
                link = a['href']
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title}.xlsx', 'wb') as f:
                        f.write(r.content)
                    try :
                        BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title}.xlsx'
                        BodyDict["JsonDetails"]["table"] = title
                        BodyDict["JsonDetails"]["description"] = title
                        BodyDict["JsonDetails"]["concept"] = concept
                        BodyDict["JsonDetails"]["topic"] = page_header 
                        BodyDict["JsonDetails"]["tags"][0]["name"] = topic
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
        elif t.text == 'SDG Indicator 10.7.2 on Migration Policies':
            topic = t.text
            print(topic)
            link = t.find_element(By.TAG_NAME,'a').get_attribute('href')
            driver.get(link)
            page_source = driver.page_source        
            soup = BeautifulSoup(page_source, 'html.parser')
            concept = soup.find(class_='col-md-8 radix-layouts-content panel-panel').text.split('Related links')[0].replace('\n',' ').replace('\n\n','')
            content = soup.find(class_="col-md-4 radix-layouts-sidebar panel-panel")
            part_content = content.find(class_="panel-panel-inner")
            part_2_content = part_content.find( class_="pane-content")
            page_header = soup.find(class_='page-header').text
            all_li = part_2_content.find_all('li')
            for li in all_li:
                start_title = li.text.partition('[')[0]
                all_a = part_2_content.find_all('a')
            for a in all_a:
                title = page_header +' '+ a.text 
                link = a['href']
                if link.endswith('xlsx'):

                    header = {
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                                "X-Requested-With": "XMLHttpRequest"
                                }
                    r = requests.get(link, headers=header)
                    with open(f'{base_path}\\{title}.xlsx', 'wb') as f:
                        f.write(r.content)
                    if 'Country data' in title:
                        import pandas as pd
                        exp_notes = []
                        notes = []
                        exp_note = ""
                        file = f'{base_path}\\{title}.xlsx'
                        xls = pd.ExcelFile(file)
                        sheets = xls.sheet_names
                        for sheet in sheets:
                            if 'contents' in sheet:
                                print(sheet)
                            elif 'notes' in sheet:
                                df_exp_note = pd.read_excel(file,sheet)
                                df_exp_note = df_exp_note[11:]
                                for i in range(len(df_exp_note.index)):
                                    f = df_exp_note.iloc[i].to_list()
                                    f = [' : '.join(map(str,f))]
                                    exp_notes.append(f[0])
                                exp_note = [' ,\n '.join(map(str,exp_notes))][0]
                            else:
                                df_data = pd.read_excel(file,sheet)
                                title_sdg = df_data.iloc[5,3].replace(':','')
                                df_data.iloc[10:13] = df_data.iloc[10:13].fillna(method='ffill', axis=1)
                                df_data.iloc[10:13] = df_data.iloc[10:13].fillna('')
                                df_data.columns = df_data.iloc[10:13].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                                ind = df_data.iloc[:,0].index[df_data.iloc[:,0]== 'Notes: '].to_list()
                                df_note = df_data[ind[0]:]
                                for i in range(len(df_note.index)):
                                    f = df_note.iloc[i].to_list()
                                    notes.append(f[0])
                                note = [' ,\n '.join(map(str,notes))][0]
                                df_data = df_data.iloc[13:,1:]
                                df_data.to_excel(f'{base_path}\\{title_sdg}.xlsx',index=False)
                                try :
                                    BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title_sdg}.xlsx'
                                    BodyDict["JsonDetails"]["table"] = title_sdg
                                    if exp_note == "":
                                        exp_note = title
                                    BodyDict["JsonDetails"]["description"] = note + '\n' + exp_note
                                    BodyDict["JsonDetails"]["concept"] = concept
                                    BodyDict["JsonDetails"]["topic"] = page_header
                                    BodyDict["JsonDetails"]["tags"][0]["name"] =  topic
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
                        import pandas as pd
                        exp_notes = []
                        notes = []
                        exp_note =  ""
                        file = f'{base_path_test}\\{title}.xlsx'
                        xls = pd.ExcelFile(file)
                        sheets = xls.sheet_names
                        for sheet in sheets:
                            if 'contents' in sheet:
                                print(sheet)
                            elif 'notes' in sheet:
                                df_exp_note = pd.read_excel(file,sheet)
                                df_exp_note = df_exp_note[11:]
                                for i in range(len(df_exp_note.index)):
                                    f = df_exp_note.iloc[i].to_list()
                                    f = [' : '.join(map(str,f))]
                                    exp_notes.append(f[0])
                                exp_note = [' ,\n '.join(map(str,exp_notes))][0]
                            else:
                                df_data = pd.read_excel(file,sheet)
                                title_sdg = df_data.iloc[5,0].replace(':','')
                                df_data.iloc[10:13] = df_data.iloc[10:13].fillna(method='ffill', axis=1)
                                df_data.iloc[10:13] = df_data.iloc[10:13].fillna('')
                                df_data.columns = df_data.iloc[10:13].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
                                ind = df_data.iloc[:,0].index[df_data.iloc[:,0]== 'Notes: '].to_list()
                                df_note = df_data[ind[0]:]
                                for i in range(len(df_note.index)):
                                    f = df_note.iloc[i].to_list()
                                    notes.append(f[0])
                                note = [' ,\n '.join(map(str,notes))][0]
                                df_data = df_data.iloc[13:,1:]
                                df_data.to_excel(f'{base_path}\\{title_sdg}.xlsx',index=False)
                                try :
                                    BodyDict["JobPath"] = f'//10.30.31.77/data_collection_dump/RawData/DESA_part_3/{title_sdg}.xlsx'
                                    BodyDict["JsonDetails"]["table"] = title_sdg
                                    if exp_note == "":
                                        exp_note = title
                                    BodyDict["JsonDetails"]["description"] = note + '\n' + exp_note
                                    BodyDict["JsonDetails"]["concept"] = concept
                                    BodyDict["JsonDetails"]["topic"] =  page_header
                                    BodyDict["JsonDetails"]["tags"][0]["name"] =topic
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

execute()


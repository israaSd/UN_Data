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
import time
import os
from selenium.webdriver.common.action_chains import ActionChains
import logging
import pandas as pd
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\EIGE'

def execute():
    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/EIGE_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\EIGE" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('ignore-certificate-errors')
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    driver.get('https://eige.europa.eu/gender-statistics/dgs') 
    time.sleep(3)

    driver.find_element(By.ID,'popup-buttons').find_element(By.TAG_NAME,'button').click()
    time.sleep(1)

    dataset_links = []
    content = driver.find_element(By.CLASS_NAME,'tree')
    driver.execute_script("arguments[0].scrollIntoView();", content)
    time.sleep(1)
    tops = content.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
    for top in tops:
        top_more = top.find_element(By.TAG_NAME,'div')
        driver.execute_script("arguments[0].scrollIntoView();", top_more)
        time.sleep(1)
        top_more.click()
        time.sleep(1)
        headers = top.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
        for header in headers:
            header_more = header.find_element(By.TAG_NAME,'div')
            driver.execute_script("arguments[0].scrollIntoView();", header_more)
            time.sleep(1)
            header_name = header_more.find_element(By.TAG_NAME,'a').text
            header_more.click()
            time.sleep(1)
            try:
                pops = header.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
                for pop in pops:
                    pop_more = pop.find_element(By.TAG_NAME,'div')
                    driver.execute_script("arguments[0].scrollIntoView();", pop_more)
                    time.sleep(1)
                    pop_name = pop_more.find_element(By.TAG_NAME,'a').text
                    pop_more.click()
                    time.sleep(1)
                    try:
                        childs = pop.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
                        for child in childs:
                            child_more = child.find_element(By.TAG_NAME,'div')
                            driver.execute_script("arguments[0].scrollIntoView();", child_more)
                            time.sleep(1)
                            child_name = child_more.find_element(By.TAG_NAME,'a').text
                            child_more.click()
                            time.sleep(1)
                            try:
                                descendants = child.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
                                for descendant in descendants:
                                    descendant_more = descendant.find_element(By.TAG_NAME,'div')
                                    driver.execute_script("arguments[0].scrollIntoView();", descendant_more)
                                    time.sleep(1)
                                    descendant_name = descendant_more.find_element(By.TAG_NAME,'a').text
                                    descendant_more.click()
                                    time.sleep(1)
                                    try:
                                        nodes = descendant.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')
                                        for node in nodes:
                                            node_more = node.find_element(By.TAG_NAME,'div')
                                            driver.execute_script("arguments[0].scrollIntoView();", node_more)
                                            time.sleep(1)
                                            node_name = node_more.find_element(By.TAG_NAME,'a').text
                                            node_more.click()
                                            time.sleep(1)
                                    except:
                                        datasets = descendant.find_elements(By.CLASS_NAME,'indicator')
                                        for dataset in datasets:
                                            dataset_content = dataset.find_element(By.TAG_NAME,'a')
                                            dataset_name = dataset_content.text
                                            dataset_link = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
                                            if dataset_link == 'https://eige.europa.eu/gender-statistics/dgs#':
                                                pass
                                            elif dataset_link in dataset_links:
                                                pass
                                            else:
                                                dataset_name = dataset_name.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ')
                                                # file_exis = dataset_name +'.csv'
                                                # if file_exis in os.listdir(base_path):
                                                #     print('the file exist')
                                                # else:
                                                dataset_links.append(dataset_link)
                                            
                            except:
                                datasets = child.find_elements(By.CLASS_NAME,'indicator')
                                for dataset in datasets:
                                    dataset_content = dataset.find_element(By.TAG_NAME,'a')
                                    dataset_name = dataset_content.text
                                    dataset_link = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
                                    if dataset_link == 'https://eige.europa.eu/gender-statistics/dgs#':
                                        pass
                                    elif dataset_link in dataset_links:
                                        pass
                                    else:
                                        dataset_name = dataset_name.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ')
                                        # file_exis = dataset_name +'.csv'
                                        # if file_exis in os.listdir(base_path):
                                        #     print('the file exist')
                                        # else:
                                        dataset_links.append(dataset_link)
                                    
                    except:
                        datasets = pop.find_elements(By.CLASS_NAME,'indicator')
                        for dataset in datasets:
                            dataset_content = dataset.find_element(By.TAG_NAME,'a')
                            dataset_name = dataset_content.text
                            dataset_link = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
                            if dataset_link == 'https://eige.europa.eu/gender-statistics/dgs#':
                                pass
                            elif dataset_link in dataset_links:
                                pass
                            else:
                                dataset_name = dataset_name.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ')
                                # file_exis = dataset_name +'.csv'
                                # if file_exis in os.listdir(base_path):
                                #     print('the file exist')
                                # else:
                                dataset_links.append(dataset_link)
            except:
                datasets = header.find_elements(By.CLASS_NAME,'indicator')
                for dataset in datasets:
                    dataset_content = dataset.find_element(By.TAG_NAME,'a')
                    dataset_name = dataset_content.text
                    dataset_link = dataset.find_element(By.TAG_NAME,'a').get_attribute('href')
                    if dataset_link == 'https://eige.europa.eu/gender-statistics/dgs#':
                        pass
                    elif dataset_link in dataset_links:
                        pass
                    else:
                        dataset_name = dataset_name.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ')
                        # file_exis = dataset_name +'.csv'
                        # if file_exis in os.listdir(base_path):
                        #     print('the file exist')
                        # else:
                        dataset_links.append(dataset_link)
        content = driver.find_element(By.CLASS_NAME,'tree')
        tops = content.find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME,'li')

    for link in dataset_links:
        driver.get(link)
        time.sleep(30)
        delay = 30
        try:
            myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'filters')))
        except TimeoutException:
            print("Loading metadata took too much time!")
        try:
            maincontain = driver.find_element(By.CLASS_NAME,'main-content')
            und = maincontain.find_element(By.CLASS_NAME,'breadcrumb')
            driver.execute_script("arguments[0].scrollIntoView();", und)
            time.sleep(1)
            under = und.find_element(By.TAG_NAME,'ul').text.replace('\n',' > ')
            try:
                keyword = driver.find_element(By.CLASS_NAME,'keywords')
                driver.execute_script("arguments[0].scrollIntoView();", keyword)
                time.sleep(1)
                tags = keyword.find_element(By.TAG_NAME,'ul').text.split('\n')
                tags = [tags,'','']
            except:
                tags = under.split(' >')[0]

            try:
                tags = [tags[0],tags[1],tags[2]]
            except:
                try:
                    tags = [tags[0],tags[1],'']
                except:
                    tags = [tags[0],'','']
            tag_1 = tags[0]
            tag_2 = tags[1]
            tag_3 = tags[2]
            pagetitle = driver.find_element(By.ID,'page-title')
            driver.execute_script("arguments[0].scrollIntoView();", pagetitle)
            time.sleep(1)
            title = pagetitle.text
            export = driver.find_element(By.CLASS_NAME,'button.export.small')
            driver.execute_script("arguments[0].scrollIntoView();", export)
            time.sleep(1)
            export.click()
            time.sleep(1)
            csv = driver.find_element(By.CLASS_NAME,'csv')
            csv.click()
            time.sleep(2)
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
            # try:
            file_name = title.replace('/',' ').replace(':',' ').replace('!',' ').replace('?',' ').replace('[',' ').replace(']',' ').replace("'",' ').replace('"',' ')
            if len(file_name)>180:
                file_name = file_name[:181]
            else:
                pass

            os.rename(file,f'{base_path}\\{file_name}.csv')
            time.sleep(1)
            df = pd.read_csv(f'{base_path}\\{file_name}.csv')
            df = df.loc[:,~df.columns.str.startswith('_')]
            df.to_csv(f'{base_path}\\{file_name}.csv',index=False)
            submenu = driver.find_element(By.CLASS_NAME,'submenu-tabs')
            driver.execute_script("arguments[0].scrollIntoView();", submenu)
            time.sleep(1)
            metadata = driver.find_element(By.CLASS_NAME,'chart.metadata').find_element(By.TAG_NAME,'a') 
            metadata.click()
            time.sleep(2)
            delay = 3 # seconds
            try:
                myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'tab.metadata')))
            except TimeoutException:
                print("Loading metadata took too much time!")
            source_name = driver.find_elements(By.TAG_NAME,'td')[1].text
            source_url = driver.find_elements(By.TAG_NAME,'td')[2].text
            limitation = driver.find_elements(By.TAG_NAME,'td')[3].text
            import datetime
            last_update = driver.find_elements(By.TAG_NAME,'td')[7].text
            last_update = datetime.datetime.strptime(last_update, "%d.%m.%Y").strftime('%Y-%m-%d')
            description = driver.find_element(By.CLASS_NAME,'description').text.split('\n')[1]
            try:
                description = driver.find_element(By.CLASS_NAME,'description').text.split('\n')[1]
            except :
                description = title
            if source_name == 'European Institute for Gender Equality (EIGE)':
                source_name = ''
                source_url = ''
            try:
                BodyDict = {
                "JobPath":f"//10.30.31.77/data_collection_dump/RawData/EIGE/{file_name}.csv", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "third-parties",
                        "source": "EIGE",
                        "source_description" : "The European Institute for Gender Equality (EIGE) is an EU agency working to make gender equality a reality in the EU and beyond.",
                        "source_url" : "https://eige.europa.eu/gender-statistics/dgs",
                        "table" : title,
                        "description" : description ,
                        ## Optional
                        "JobType": "JSON",
                        "CleanPush": True,
                        "Server": "str",
                        "UseJsonFormatForSQL":  False,
                        "CleanReplace":True,
                        "MergeSchema": False,
                        "tags": [
                                    {"name": tag_1},
                                    {"name": tag_2},
                                    {"name": tag_3}
                                ],
                        "additional_data_sources": [{       
                                "name": source_name,        
                                "url": source_url  ## this object will be ignored if "name" is empty    }
                        }],
                        "limitations":limitation,
                        "concept":  under,
                        "periodicity": '',
                        "topic": title ,
                        "created": last_update,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified":last_update ,                #* ""               ""                  ""              ""
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
        except Exception as err:
            print(err)
            logging.info(f"No export data - {link} ")
execute()
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNFCCC\\Comparison by Gas"
import logging
from selenium.webdriver.support.ui import Select
from Hashing.HashScrapedData import _hashing
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
import time
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

chrome_options = webdriver.ChromeOptions()
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\UNFCCC\\Comparison by Gas" # local, gets current working directory
prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
chrome_options.add_experimental_option('prefs', prefs)
# chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.maximize_window()

driver.get('https://di.unfccc.int/comparison_by_gas')
time.sleep(4)

content = driver.find_element(By.CLASS_NAME,'detailed-party-view')
topic = content.find_element(By.TAG_NAME,'span').text
Select_elements = driver.find_elements(By.CLASS_NAME,'party-data-filter')
# for select_element in select_1:
select_element1 = Select_elements[0]
select_element2 = Select_elements[1]
select_element3 = Select_elements[2]
select_element4 = Select_elements[3]
select_element5 = Select_elements[4]
select_1 = Select(select_element1)
option_list_1 = select_1.options
for op_1 in option_list_1[1:]:
    select_1.select_by_visible_text(op_1.text)
    t_1 = op_1.text
    time.sleep(1)
    select_2 = Select(select_element2)
    option_list_2 = select_2.options
    for op_2 in option_list_2[1:]:
        print(op_2.text)
        # select_2.select_by_visible_text(op_2.text)
        if op_2.text == op_1.text :
            pass
        else:
            select_2.select_by_visible_text(op_2.text)
            t_2 = op_2.text
            time.sleep(2)
            select_3 = Select(select_element3)
            option_list_3 = select_3.options
            for op_3 in option_list_3:
                select_3.select_by_visible_text(op_3.text)
                t_3 = op_3.text
                time.sleep(2)
                select_5 = Select(select_element5)
                option_list_5 = select_5.options
                for op_5 in option_list_5[1:]:
                    try:
                        select_5.select_by_visible_text(op_5.text)
                        t_5 = op_5.text
                        time.sleep(2)
                        select_4 = Select(select_element4)
                        option_list_4 = select_4.options
                        for op_4 in option_list_4[1:]:
                            if op_4.text == op_5.text:
                                pass
                            else:
                                select_4.select_by_visible_text(op_4.text)
                                t_4 = op_4.text
                                time.sleep(4)
                                title = 'Comparison by Gas' + ' ' + t_1 + ' and ' + t_2 + ' ' + t_3 + ', ' + t_4 + 'and ' + t_5
                                title = title.replace('.','').replace('/','').replace(':',' ').replace('₂','2').replace('₆','6').replace('₃','3').replace('₄','4')
                                unt = driver.find_element(By.XPATH,"//*[contains(text(), 'Query results for')]").text.split('— Unit:')[1]
                                table = driver.find_element(By.CLASS_NAME,'dataTable.no-footer')
                                thead = table.find_element(By.TAG_NAME,'thead').find_elements(By.TAG_NAME,'tr')
                                tr_1 = thead[0].find_elements(By.TAG_NAME,'th')
                                tr_2 = thead[1].find_elements(By.TAG_NAME,'th')
                                head_1 = []
                                for th_1 in tr_1:
                                    head_1.append(th_1.text)
                                head_2 = []
                                for th_2 in tr_2:
                                    head_2.append(th_2.text)
                                column = [f'{head_1[0]}',f'{head_1[1]} {head_2[0]}',f'{head_1[1]} {head_2[1]}', f'{head_1[1]} {head_2[2]}',f'{head_1[2]} {head_2[3]}',f'{head_1[2]} {head_2[4]}',f'{head_1[2]} {head_2[5]}',f'{head_1[3]} {head_2[6]}',f'{head_1[3]} {head_2[7]}']
                                rows = table.find_elements(By.TAG_NAME,'tr')
                                l = []
                                for tr in rows[2:]:
                                    cells = tr.find_elements(By.TAG_NAME,'td')
                                    cells_text = [cell.text for cell in cells]
                                    l.append(cells_text)
                                df = pd.DataFrame(l,columns=column) 
                                unit = [unt] * len(df.index) 
                                df.insert(1,'Unit',unit)
                                df.to_excel(f'{base_path}\\{title}.xlsx',index = False)
                                try:
                                    BodyDict = {
                                    "JobPath":f'//10.30.31.77/data_collection_dump/RawData/UNFCCC/Comparison by Gas/{title}.xlsx', #* Point to downloaded data for conversion #
                                    "JsonDetails":{
                                            ## Required
                                            "organisation": "un-agencies",
                                            "source": "UNFCCC",
                                            "source_description" : "The United Nations Framework Convention on Climate Change established an international environmental treaty to combat 'dangerous human interference with the climate system', in part by stabilizing greenhouse gas concentrations in the atmosphere.",
                                            "source_url" : "https://di.unfccc.int",
                                            "table" : title ,
                                            "description" : notes  ,
                                            ## Optional
                                            "JobType": "JSON",
                                            "CleanPush": True,
                                            "Server": "str",
                                            "UseJsonFormatForSQL":  False,
                                            "CleanReplace":True,
                                            "MergeSchema": False,
                                            "tags": [
                                                        {"name": 'Comparison by Gas'}
                                                    ],
                                            "additional_data_sources": [{       
                                                    "name": '',        
                                                    "url": ''  ## this object will be ignored if "name" is empty    }
                                            }],
                                            "limitations":'',
                                            "concept":  '',
                                            "periodicity":  '',
                                            "topic": topic + ', ' + title ,
                                            "created": '',                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                            "last_modified":'' ,                #* ""               ""                  ""              ""
                                            "TriggerTalend" :  False,    #* initialise to True for production
                                            "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/UNFCCC/Comparison by Gas" #* initialise as empty string for production.
                                        }
                                    }
                                    # tablenom = BodyDict['JsonDetails']['table']

                                    # hashmessage = _hashing(BodyDict['JsonDetails']['source'], tablenom, BodyDict["JobPath"])

                                    # if hashmessage["Trigger_InferSchema"] == True and hashmessage["Success"] == True:

                                    TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
                                    TriggerInferShemaToJsonAPIClass.TriggerAPI()
                                    logging.info(f"Conversion successful - {title} ")
                                    print(BodyDict)
                                    #     logging.info(f"Conversion successful - {tablenom}, hashmessage: {hashmessage['message']}")

                                    # # logging.info(f"Conversion successful for {dataset}")
                                    # elif hashmessage['Success'] == True and hashmessage['Trigger_InferSchema'] == False:

                                    #     # dont trigger conversion nor talend
                                    #     logging.info(f"{hashmessage['message']}")

                                    # elif hashmessage['Success'] == False:
                                    #     logging.info(f"Hashing error or Unexpected Issue: {hashmessage['message']}")
                                except  Exception as err:
                                    print(err)
                                notes = driver.find_element(By.CLASS_NAME,'disclaimer-notes').text
                                print(notes)
                    except  Exception as err:
                        print(err)


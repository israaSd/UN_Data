from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "C:\\Users\\10235555\\Documents\\Dataportal Data\\reports EG survey T"
import urllib.request
import logging
import pandas as pd
from selenium.webdriver.support.ui import Select
from re import search



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
from pyshadow.main import Shadow
from py7zr import unpack_7zarchive
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI

def execute():
    logging.basicConfig(handlers=[logging.FileHandler(f"{base_path}/reports_logs.log"),
        logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "C:\\Users\\10235555\\Documents\\Dataportal Data\\reports EG survey T" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    # chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()


    url = 'https://publicadministration.un.org/egovkb/en-us/Reports'
    driver.get(url)

    header = driver.find_element(By.ID,'navdttg')
    ul_header = header.find_element(By.CLASS_NAME,'nav.nav-pills')
    list_header = ul_header.find_elements(By.CLASS_NAME,'dropdown ')
    reports = list_header[4]
    survey_2020 = reports.find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')[1].find_element(By.TAG_NAME,'a').get_attribute('href')
    driver.get(survey_2020)

    content = driver.find_element(By.ID,'dnn_ctr1445_ContentPane')
    title = driver.find_element(By.TAG_NAME,'h2').text
    publication = content.find_element(By.TAG_NAME,'a').get_attribute('href')
    urllib.request.urlretrieve(publication,f"{base_path}\\{title}.pdf")

    driver.back()
    time.sleep(1)

    for i in range(2,5):
        header = driver.find_element(By.ID,'navdttg')
        ul_header = header.find_element(By.CLASS_NAME,'nav.nav-pills')
        list_header = ul_header.find_elements(By.CLASS_NAME,'dropdown ')
        reports = list_header[4]
        survey_2018_2014 = reports.find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'a').get_attribute('href')
        driver.get(survey_2018_2014)

        content = driver.find_element(By.CLASS_NAME,'Normal')
        title = driver.find_element(By.TAG_NAME,'h2').text
        time.sleep(2)
        publication = content.find_element(By.XPATH,"//*[contains(text(), 'English')]").get_attribute('href')
        print(publication)
        urllib.request.urlretrieve(publication,f"{base_path}\\{title}.pdf")
        driver.back()
        time.sleep(1)
        

    for i in range(5,10):
        header = driver.find_element(By.ID,'navdttg')
        ul_header = header.find_element(By.CLASS_NAME,'nav.nav-pills')
        list_header = ul_header.find_elements(By.CLASS_NAME,'dropdown ')
        reports = list_header[4]
        survey_2012_2004 = reports.find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')[i].find_element(By.TAG_NAME,'a').get_attribute('href')
        driver.get(survey_2012_2004)
        content = driver.find_element(By.CLASS_NAME,'Normal')
        title = driver.find_element(By.TAG_NAME,'h2').text
        publication = content.find_element(By.XPATH,"//*[contains(text(), 'Download Publication')]").get_attribute('href')
        urllib.request.urlretrieve(publication,f"{base_path}\\{title}.pdf")
        driver.back()
        time.sleep(1)

    # report 2020
    import camelot
    tables = camelot.read_pdf(f"{base_path}\\UN E-Government Survey 2020.pdf", pages='349,350,351,352,353')
    df_0 = tables[0].df.replace('\n','', regex=True)
    df_0.columns = df_0.iloc[0]
    df_0 = df_0.iloc[1:]
    df_1 = tables[1].df.replace('\n','', regex=True)
    df_1.columns = df_1.iloc[0]
    df_1 = df_1.iloc[1:]
    df_2 = tables[2].df.replace('\n','', regex=True)
    df_2.columns = df_2.iloc[0]
    df_2 = df_2.iloc[1:]
    df_3 = tables[3].df.replace('\n','', regex=True)
    df_3.columns = df_3.iloc[0]
    df_3 = df_3.iloc[1:]
    df_4 = tables[4].df.replace('\n','', regex=True)
    df_4.columns = df_4.iloc[0]
    df_4 = df_4.iloc[1:]
    df = pd.concat([df_0,df_1,df_2,df_3,df_4], ignore_index=True)
    df.to_csv(f'{base_path}\\E-Participation Index (EPI) and its utilisation by stages 2020.csv',index=False)
    logging.info(f" Saved file - E-Participation Index (EPI) and its utilisation by stages 2020")

    # report 2018
    tables = camelot.read_pdf(f"{base_path}\\UN E-Government Survey 2018.pdf", flavor='stream', pages='275,276,277,278,279')
    df_0 = tables[0].df.replace('\n','', regex=True)
    df_0.columns = df_0.iloc[1]
    df_0 = df_0.iloc[2:]
    df_1 = tables[2].df.replace('\n','', regex=True)
    df_1.columns = df_1.iloc[1]
    df_1 = df_1.iloc[2:]
    df_2 = tables[4].df.replace('\n','', regex=True)
    df_2.columns = df_2.iloc[1]
    df_2 = df_2.iloc[2:]
    df_3 = tables[5].df.replace('\n','', regex=True)
    df_3.columns = df_3.iloc[1]
    df_3 = df_3.iloc[2:]
    df_4 = tables[7].df.replace('\n','', regex=True)
    df_4.columns = df_4.iloc[1]
    df_4 = df_4.iloc[2:]
    df = pd.concat([df_0,df_1,df_2,df_3,df_4], ignore_index=True)
    df.iloc[182:184] = df.iloc[182:184].fillna('')
    df.iloc[182] = df.iloc[182:184].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df= df.drop([df.index[183]])
    df.to_csv(f"{base_path}\\E-Participation Index (EPI) and its utilisation by stages 2018.csv",index=False)
    logging.info(f" Saved file - E-Participation Index (EPI) and its utilisation by stages 2018")

    # report 2016
    tables = camelot.read_pdf(f"{base_path}\\UN E-Government Survey 2016.pdf", flavor='stream', pages='191,192,193,194,195')
    df_0 = tables[0].df.replace('\n','', regex=True)
    df_0.columns = df_0.iloc[1]
    df_0 = df_0.iloc[2:]
    df_1 = tables[1].df.replace('\n','', regex=True)
    df_1.columns = df_1.iloc[1]
    df_1 = df_1.iloc[2:]
    df_2 = tables[2].df.replace('\n','', regex=True)
    df_2.columns = df_2.iloc[1]
    df_2 = df_2.iloc[2:]
    df_3 = tables[3].df.replace('\n','', regex=True)
    df_3.columns = df_3.iloc[1]
    df_3 = df_3.iloc[2:]
    df_4 = tables[4].df.replace('\n','', regex=True)
    df_4.columns = df_4.iloc[1]
    df_4 = df_4.iloc[2:]
    df = pd.concat([df_0,df_1,df_2,df_3,df_4], ignore_index=True)
    df.to_csv(f"{base_path}\\E-Participation Index (EPI) and its utilisation by stages 2016.csv",index=False)
    logging.info(f" Saved file - E-Participation Index (EPI) and its utilisation by stages 2016")

    # report 2014
    tables = camelot.read_pdf(f"{base_path}\\UN E-Government Survey 2014.pdf", flavor='stream', pages='257,258,259,260,261')
    df_0 = tables[0].df.replace('\n','', regex=True)
    df_0.columns = df_0.iloc[1]
    df_0 = df_0.iloc[2:]
    df_1 = tables[1].df.replace('\n','', regex=True)
    df_1.columns = df_1.iloc[1]
    df_1 = df_1.iloc[2:]
    df_2 = tables[2].df.replace('\n','', regex=True)
    df_2.columns = df_2.iloc[1]
    df_2 = df_2.iloc[2:]
    df_3 = tables[3].df.replace('\n','', regex=True)
    df_3.columns = df_3.iloc[1]
    df_3 = df_3.iloc[2:]
    df_4 = tables[4].df.replace('\n','', regex=True)
    df_4.columns = df_4.iloc[1]
    df_4 = df_4.iloc[2:]
    df = pd.concat([df_0,df_1,df_2,df_3,df_4], ignore_index=True)
    df.columns.values[0] = "Rank"
    df.to_csv(f"{base_path}\\E-Participation Index and its utilisation by stages 2014.csv",index=False)
    logging.info(f" Saved file - E-Participation Index and its utilisation by stages 2014")

    # report 2012
    import camelot
    tables = camelot.read_pdf(f"C:\\Users\\10235555\\Documents\\Dataportal Data\\reports EG survey T\\United Nations E-Government Survey 2012.pdf", flavor='stream', pages='60')
    df = tables[0].df
    df.iloc[1:3] = df.iloc[1:3].fillna(method='ffill', axis=1)
    df.iloc[1:3] = df.iloc[1:3].fillna('')
    df.columns = df.iloc[1:3].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df = df.iloc[3:,3:]
    df.iloc[66:69] = df.iloc[66:69].fillna('')
    df.iloc[66] = df.iloc[66:69].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df.iloc[86:89] = df.iloc[86:89].fillna('')
    df.iloc[86] = df.iloc[86:89].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df = df.drop([df.index[67], df.index[68] , df.index[87], df.index[88] ])
    df.to_csv(f'{base_path}\\Extent of e-participation 2012.csv',index=False)
    logging.info(f" Saved file - Extent of e-participation 2012")

    # report 2010
    tables = camelot.read_pdf(f"{base_path}\\United Nations E-Government Survey 2010.pdf", flavor='stream', pages='101')
    df_10 = tables[0].df
    df_10.columns = df_10.iloc[3]
    df_10.iloc[5:,3:].to_csv(f'{base_path}\\Quality of e-participation websites of selected countries 2010.csv',index=False )
    logging.info(f" Saved file - Quality of e-participation websites of selected countries 2010")

    # report 2008
    tables = camelot.read_pdf(f"{base_path}\\Global E-Government Survey 2008.pdf", flavor='stream', pages='81,82')
    df_0 = tables[0].df
    df_0 = df_0.iloc[2:]
    df_0.columns = df_0.iloc[1]
    df_0.columns.values[4] = df_0.iloc[0,4] + ' ' + df_0.iloc[2,4]
    df_0 = df_0[3:]
    df_1 = tables[1].df
    df_1.columns = df_1.iloc[1]
    df_1.columns.values[4] = df_1.columns[4] + ' ' + df_1.iloc[2,4]
    df_1 = df_1[3:]
    df = pd.concat([df_0,df_1], ignore_index=True)
    df = df.iloc[:,1:]
    df.to_csv(f'{base_path}\\Quality and Relevance of e-Participation Initiative, Selected Countries 2008.csv',index=False)
    logging.info(f" Saved file - Quality and Relevance of e-Participation Initiative, Selected Countries 2008")

    # report 2005
    tables = camelot.read_pdf(f"{base_path}\\Global E-Government Development Report 2005.pdf", flavor='stream', pages='114')
    df = tables[0].df
    df.columns = df.iloc[1]
    df.columns.values[0] = 'country'
    df = df[2:]
    df.iloc[6:9] = df.iloc[6:9].fillna('')
    df.iloc[6] = df.iloc[6:9].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df.iloc[38:41] = df.iloc[38:41].fillna('')
    df.iloc[38] = df.iloc[38:41].apply(lambda x: ' '.join(map(str,[y for y in x if y])), axis=0)
    df= df.drop([df.index[7],df.index[8],df.index[39],df.index[40]])
    df.to_csv(f'{base_path}\\ Quality and relevance of e-participation initiatives, selected countries 2005.csv',index=False)
    logging.info(f" Saved file - Quality and relevance of e-participation initiatives, selected countries 2005")

    # report 2004
    tables = camelot.read_pdf(f"{base_path}\\Global E-Government Development Report 2004.pdf", flavor='stream', pages='84')
    df = tables[1].df
    df = df[4:]
    df.columns = df.iloc[1]
    df.columns.values[0] = 'country'
    df = df[2:]
    df.to_csv(f'{base_path}\\Quality and Relevance of e-participation, selected countries 2004.csv',index=False)
    logging.info(f" Saved file - Quality and relevance of e-participation initiatives, selected countries 2004")

# execute()
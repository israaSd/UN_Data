import requests
from bs4 import BeautifulSoup
import wget
import logging
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
import os
import urllib.request
from urllib import request
import pdfplumber
import tabula 
import csv
import pandas as pd
from unidecode import unidecode
from Hashing.HashScrapedData import _hashing
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNSTATS_YearBook2021'

def execute():

    logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/unstats_yearbook2021.log"),
                              logging.StreamHandler()], level=logging.INFO)

    result = requests.get('https://unstats.un.org/unsd/demographic-social/products/dyb/dyb_2021/')
    soup = BeautifulSoup(result.content,'html.parser')

    index=[0,6,11,15,19,23,29,32]

    for i in index:
        url = "https://unstats.un.org"
        parent_node = soup.find(class_ = 'sub1')
        child_node = parent_node.find_all('li')[i]
        files = child_node.find('ul',{'class':'leaf'})
        #top =child_node.find_all("li")[files.index(files)].text
        tags = child_node.text.partition('\r')[0]
        table = files.find_all('li')
        for t in table:
            title = t.text.partition('\n')[0].replace('/', ' or ').replace('\t',' ').replace(':','')
            link_xls = t.find_all('a')[1]['href']
            link_xls = str(url) + link_xls
            link_pdf = t.find_all('a')[2]['href']
            link_pdf = str(url) + link_pdf
            conten = []
            urllib.request.urlretrieve(link_xls,f"{base_path}\\{title}.xls")
            try:
                urllib.request.urlretrieve(link_pdf,f"{base_path}\\{title}.pdf")
            except:
                pass


            BodyDict = {
                "JobPath":"", #* Point to downloaded data for conversion #
                "JsonDetails":{
                        ## Required
                        "organisation": "un-agencies",
                        "source": "UNSTATS",
                        "source_description" : "The Demographic Yearbook 2021 is the seventy-second issue in a series published by the United Nations since 1948. It contains tables on a wide range of demographic statistics, including a world summary of selected demographic statistics, statistics on the size, distribution and trends in national populations,fertility, foetal mortality, infant and maternal mortality, general mortality, nuptiality and divorce. Data are shown by urban/rural residence, as available. The volume provides Technical Notes, a synoptic table, a historical index and a listing of the issues of the Demographic Yearbook published to date. This issue of Demographic Yearbook contains data as available including reference year 2021.",
                        "source_url" : "https://unstats.un.org/unsd/demographic-social/products/dyb/dyb_2021/",
                        "table" : title,
                        "description" : "",
                        ## Optional
                        "JobType": "JSON",
                        "CleanPush": True,
                        "Server": "str",
                        "UseJsonFormatForSQL":  False,
                        "CleanReplace":True,
                        "MergeSchema": False,
                        "tags": [{
                            "name": tags
                        }],
                        "additional_data_sources": [{
                            "name": ""
                        }],
                        "limitations":"",
                        "concept":  "",
                        "periodicity":  "",
                        "topic":  "",
                        "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                        "last_modified": "",                #* ""               ""                  ""              ""
                        "TriggerTalend" :  True,    #* initialise to True for production
                        "SavePathForJsonOutput": "" #* initialise as empty string for production.
                    }
                }
            if title.startswith('Table 3a'):
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                title = 'Table 3a - Whipple s Index by sex and urban or rural residence, 1985-2021'
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = title
                    # TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
                    # TriggerInferShemaToJsonAPIClass.TriggerAPI()
                    # logging.info(f"Conversion successful - {title} ")
                    # print(BodyDict)
                except  Exception as err:
                    print(err)

            elif title == 'Table 1 - Population, rate of increase, birth and death rates, surface area and density for the world, major areas and regions selected years':

                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Composition of major areas and regions')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title =='Table 2 - Estimates of population and its percentage distribution, by age and sex and sex ratio for all ages for the world, major areas and regions 2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:5] = df.iloc[2:5].fillna(method='ffill', axis=1)
                df.iloc[2:5] = df.iloc[2:5].fillna('')
                df.columns = df.iloc[2:5].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[5:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                df.rename(columns={df.columns[13]: df.columns[13].replace(' - Female\nFéminin - 65+','')},inplace=True)
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of  variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of  variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('1 United Nations,')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 21 - Probability of dying in the five year interval following specified age (5qx), by sex latest available year, 2002 - 2021':

                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])

                meta_topic = content_all.partition('\nThe life')[0].replace('\n','')
                meta_description = content_all.partition('\nThe life')[2].split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept =  content_all.partition('\nThe life')[2].split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = content_all.partition('\nThe life')[2].split('Reliability of data')[1].split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 3 - Population by sex, annual rate of population increase, surface area and density':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.rename(columns={df.columns[5]: df.columns[5].replace(' - Female \n Feminin','')},inplace=True)
                df.rename(columns={df.columns[10]: df.columns[10].replace(' - 2021','')},inplace=True)
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 4 - Vital statistics summary and life expectancy at birth 2017-2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.rename(columns={df.columns[7]: df.columns[7].replace(' - Crude death rate \n Taux brut de mortalite','')},inplace=True)
                df.rename(columns={df.columns[13]: df.columns[13].replace(' - Femaleb \n Femininb','')},inplace=True)
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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


            elif title == 'Table 10 - Live births by age of mother and sex of child, general and age-specific fertility rates latest available year, 2012-2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.rename(columns={df.columns[4]: df.columns[4].replace(' - Female \n Feminin','')},inplace=True)
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 11 - Live births and live birth rates by age of father latest available year, 2012-2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.rename(columns={df.columns[2]: df.columns[2].replace(' - Both sexes \n Les deux sexes','')},inplace=True)
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 20 - Death and death rates by cause and sex 2016 - 2020':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:6] = df.iloc[2:6].fillna(method='ffill', axis=1)
                df.iloc[2:6] = df.iloc[2:6].fillna('')
                df.columns = df.iloc[2:6].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[7:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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


            elif title == 'Table 6 - Total and urban population by sex 2012-2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:5] = df.iloc[2:5].fillna(method='ffill', axis=1)
                df.iloc[2:5] = df.iloc[2:5].fillna('')
                df.columns = df.iloc[2:5].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                df.rename(columns={df.columns[5]: df.columns[5].replace(' - Percent P.100','')},inplace=True)
                df.rename(columns={df.columns[8]: df.columns[8].replace(' - Percent P.100',' ')},inplace=True)
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 8 - Population of capital cities and cities of 100 000 or more inhabitants latest available year, 2002-2021':
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:5] = df.iloc[2:5].fillna(method='ffill', axis=1)
                df.iloc[2:5] = df.iloc[2:5].fillna('')
                df.columns = df.iloc[2:5].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[5:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.rename(columns={df.columns[4]: df.columns[4].replace('- Population',' ')},inplace=True)
                df.rename(columns={df.columns[8]: df.columns[8].replace('- Population',' ')},inplace=True)
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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

            elif title == 'Table 17 - Maternal deaths and maternal mortality ratios 2011-2020':
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.columns = df.iloc[2]
                df = df[4:]
                df = df.loc[:, df.columns.notna()]
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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
                colum = []
                df = pd.read_excel(f'{base_path}\\{title}.xls')
                df.iloc[2:4] = df.iloc[2:4].fillna(method='ffill', axis=1)
                df.iloc[2:4] = df.iloc[2:4].fillna('')
                df.columns = df.iloc[2:4].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                df = df.iloc[4:]
                df = df.loc[:,~df.columns.duplicated()].copy()
                list = df.columns.to_list()
                for col in list:
                    colum.append(unidecode(col.replace('\n',' \n ')))
                df.columns = colum
                df.iloc[:,0] = df.iloc[:,0].str.encode('ascii', 'ignore').str.decode('ascii')
                df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                with pdfplumber.open(f"{base_path}\\{title}.pdf") as pdf:

                    text = pdf.pages[0:2]
                    for te in text:
                        content = te.extract_text()
                        conten.append(content)
                content_all = ' '.join([str(elem) for elem in conten])
                meta_topic = content_all.split('Description of variables')[0].replace('\n \n \n',',').replace('\n','')
                meta_descript =content_all.split('Description of variables')[1]
                meta_description = meta_descript.split('Reliability of data')[0].replace('\n','').replace('\n \n','').replace(':','')
                concept = meta_descript.split('Reliability of data')[1].split('Limitations')[0].replace('\n','').replace('\n \n','').replace(':','')
                limitations = meta_descript.split('Limitations')[1].split('Earlier data')[0].replace('\n','').replace('\n \n','').replace(':','')

                try:
                    BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNSTATS_YearBook2021/{title}.xlsx"
                    BodyDict["JsonDetails"]["table"] = title
                    BodyDict["JsonDetails"]["description"] = meta_description
                    BodyDict["JsonDetails"]["limitations"] = limitations
                    BodyDict["JsonDetails"]["concept"] = concept
                    BodyDict["JsonDetails"]["topic"] = meta_topic
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
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
import traceback
import logging
import os
import shutil
import zipfile
import time
import datetime
from zipfile import ZipFile
import pandas as pd
logging.basicConfig(handlers=[logging.FileHandler("//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/IMF/imf_logs.log"),
    logging.StreamHandler()], level=logging.INFO)
metadata_files = []
data_files = []
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\IMF'
for zipfiles in os.listdir(base_path):
    if zipfiles.endswith('zip'):
        zip = zipfile.ZipFile(f'{base_path}\\{zipfiles}')
        # with ZipFile(f'{base_path}\\{zipfiles}', 'r') as zipObject:
        files = zip.namelist()
        print(files[0])
        f = zip.open(files[1])
        df = pd.read_csv(f)
        try:
            index1 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Dataset'].tolist()
            title = df.iloc[index1[0],-2]
        except:
            title = ''
        try:
            index2 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Definition'].tolist()
            description = df.iloc[index2[0],-2]
        except:
            description = ''
        try:
            index3 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Frequency'].tolist()
            periodicity = df.iloc[index3[0],-2]
        except:
            periodicity = ''
        try:
            index4 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Latest Update Date'].tolist()
            last_modified = df.iloc[index4[0],-2]
            last_modified = datetime.datetime.strptime(last_modified, "%m/%d/%Y").strftime('%Y-%m-%d')
            created = df.iloc[index4[0],-2]
            created = datetime.datetime.strptime(created, "%m/%d/%Y").strftime('%Y-%m-%d')
        except:
            last_modified = ''
            created = ''
        try:
            index5 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Methodology'].tolist()
            concept = df.iloc[index5[0],-2]
        except:
            concept = ''
        try:
            index6 = df.iloc[:,-3].index[df.iloc[:,-3] == 'Topics'].tolist()
            topic = df.iloc[index6[0],-2]
        except:
            topic = ''
        new_name = files[0].split('_0')[0].split('_1')[0]
        with zipfile.ZipFile(f'{base_path}\\{zipfiles}') as z:
            with z.open(files[0]) as zf, open(f'{base_path}\\{new_name}.csv', 'wb') as f:
                shutil.copyfileobj(zf, f)
                # os.rename(f'{base_path}\\{files[0]}', f'{base_path}\\{new_name}.csv')
                try:
                    BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/IMF/{new_name}.csv", #* Point to downloaded data for conversion 
                        "JsonDetails":{
                                ## Required
                                "organisation": "un-agencies",
                                "source": "IMF",
                                "source_description" : "The International Monetary Fund (IMF) works to achieve sustainable growth and prosperity for all of its 190 member countries. It does so by supporting economic policies that promote financial stability and monetary cooperation, which are essential to increase productivity, job creation, and economic well-being. The IMF is governed by and accountable to its member countries.",
                                "source_url" : "https://data.imf.org/?sk=388DFA60-1D26-4ADE-B505-A05A558D9A42&sId=1479329132316",
                                "table" : new_name,
                                "description" : description,
                                ## Optional
                                "JobType": "JSON",
                                "CleanPush": True,
                                "Server": "str",
                                "UseJsonFormatForSQL":  False,
                                "CleanReplace":True,
                                "MergeSchema": False,
                                "tags": [{
                                    "name": "unclassified"
                                }],
                                "additional_data_sources": [{
                                    "name": ""
                                }],
                                "limitations":"",
                                "concept":  concept,
                                "periodicity":  periodicity,
                                "topic":  topic,
                                "created": created,                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                "last_modified": last_modified,                #* ""               ""                  ""              ""
                                "TriggerTalend" :  False,    #* initialise to True for production
                                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData" #* initialise as empty string for production.
                            }
                        }

                    TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
                    TriggerInferShemaToJsonAPIClass.TriggerAPI()
                    logging.info(f"Conversion successful - {new_name}")
                    print(BodyDict)
                except Exception as err:
                    logging.critical(f"Could not save file {new_name} : {traceback.format_exc()}")
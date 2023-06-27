from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from datetime import date
from tqdm import tqdm
import multiprocessing
import datetime as dt
import pandas as pd
import numpy as np
import traceback
import requests
import logging
import time
import json
import os
import re


# Creates a folder for today's logs

today = date.today()
log_path = "Z:\\TACFolder\\ScraperLogs"                       #* Set log path for where to save logs.out and logs.xlsx
# print(os.getcwd())
if (not (str(today) in os.listdir(f"{log_path}\\hdx_logs")) ):
    os.mkdir(f"{log_path}\\hdx_logs/"+str(today))
else:
    pass

logging.basicConfig(filename = f"{log_path}\\hdx_logs\\" + str(today) + '\\hdx_download_logs.log', filemode = 'a', level = logging.DEBUG, format = '%(asctime)s - %(levelname)s: %(message)s',\
                     datefmt = '%m/%d/%Y %I:%M:%S %p' )

logging.info("Started collecting data")

class HDXScraper(): 
    
    def __init__(self):
        self.datasets = []
        # self.work_dir = os.getcwd()
        self.work_dir = f"Z:\\RawData\\HDX_Data" #* Server
        # self.documents_status = []
    
    def intersection_exists(self, lst1, lst2):
        return len(list(set(lst1) & set(lst2))) != 0

    def configure_hdx(self):

        # setup_logging()
        Configuration.create(hdx_site="prod", user_agent="TestingAPI", hdx_read_only=True, )
        
    def get_hdx_datasets(self):
        
        # Checking for new datasets to add to our list
        # print(self.datasets)
        # missing_documents = [each for each in self.datasets if each not in documents]
        # documents = json.loads('documents.json')
        self.configure_hdx()        
        self.datasets = Dataset.get_all_dataset_names()

    def request_data_from_hdx(self,dataset):
        
        tries = 0
        data = {}
        try:

            data = Dataset.read_from_hdx(str(dataset))
            
        except:

            tries = tries+1
            if (tries<=10):
                self.request_data_from_hdx(dataset)
            else:
                logging.info("Could not request data - timeout after 10 tries!")
                return None

        # logging.info(f'Data fetched successfully {data["name"]} ')

        return data

    def download_file(self, dataset=''): #! UNCOMMENT BODYDICT TRIGGER

        if dataset:
            try:
                self.configure_hdx()
            except  Exception as E:
                # logging.info(f"error during configuration {E}")
                pass
            
            # logging.info(f'Downloading {dataset}')

            data = self.request_data_from_hdx(dataset = dataset)
            filetypes = data.get_filetypes()
            # logging.info(filetypes)
            if not filetypes:
                logging.info(f' No files to download for {dataset}')
                return None            

            name = dataset
            # try:
            #     logging.info(f' the current path is {os.getcwd()}')
            #     save_directory = f"{ self.work_dir }\\Data\\{name}"
            #     logging.info(f'Saving to {save_directory} \\n')
            #     os.mkdir(save_directory)
            #     # os.chdir(f"{ os.getcwd() }\\ZData\\{name}")   # initialize directory for each dataset and chdir to it
            # except:
            #     logging.info(f"{save_directory} - Error saving the document {name}")
                # os.chdir(f"{save_directory} - Error")
            # data = Dataset.read_from_hdx(name)
            index = self.datasets.index(name)
            missedindecies = []
            escwaISOcountries = ['dza', 'bhr', 'dji', 'com', 'egy', 'irq', 'jor', 'kwt', 'lbn', 'lby', 'mar', 'omn', 'qat', 'sau', 'syr', 'tun', 'are', 'pse', 'yem', 'sdn', 'som', 'mrt']
            dataset_countries = [each['id'] for each in data['groups']]

            if self.intersection_exists(dataset_countries, escwaISOcountries):
                try:
                    # logging.info(f' the current path is {os.getcwd()}')
                    save_directory = f"{self.work_dir}\\{name}"
                    # logging.info(f'Saving to {save_directory}')
                    os.mkdir(save_directory)
                    # os.chdir(f"{ os.getcwd() }\\ZData\\{name}")   # initialize directory for each dataset and chdir to it
                except:
                    logging.error(f"{save_directory} - Error saving the document {name}")
                try:
                    data.save_to_json(f"{save_directory}/metadata_{name}.json")
                    
                    for resource in tqdm(data.get_resources(), ascii=True, leave=None, desc=dataset): # looping through each file associated with that specific dataset
                        fileindex = data.get_resources().index(resource)
                        filemetadata = data.get_resources()[fileindex]                        
                        url = resource['download_url']                            # making sure naming convention is correct
                        try:
                            r = requests.get(url, allow_redirects=True)
                        except:
                            continue
                        filename = resource['name']
                        format = ('.' + resource['format'].lower())
                        if format == ".geoservice" or format == ".sql":
                            continue
                        if "tif" in filename or format == ".geopackage":
                            format = ""
                        elif format == ".shp" or format == ".emf" or format == ".geodatabase" or format == ".geotiff":
                            format = ".zip"
                            if format in filename:
                                format = ""
                        elif format == ".topojson":
                            format = ".json"
                            if format in filename:
                                format = ""
                        elif format in filename:
                            format = ""
                        elif ".zip" in filename:
                            format = ""
                        if os.path.splitext(filename)[-1] == "":
                            finalname = filename+format
                        else:
                            finalname = filename
                            
                        try:
                            open(f"{save_directory}/{finalname}", 'wb').write(r.content)
                            # logging.info(f'File written at {save_directory}/{finalname}')
                        except:
                            logging.error(f'Unable to write at {save_directory}/{finalname}')
                            continue
                        
                        tabular_formats = ["XLS", "XLSX", "CSV"]
                        if resource['format'] in tabular_formats:
                            try:
                                # print(filemetadata)#['download_url'])
                                if 'caveats' in data.keys():
                                    limitations = data['caveats']
                                else:
                                    limitations = ""
                                
                                BodyDict = {
                                    "JobPath":f"//10.30.31.77/data_collection_dump/RawData/HDX_Data/{name}/{finalname}", #* Point to downloaded data for conversion
                                    "JsonDetails":{
                                    ## Required
                                    "organisation": "un-agencies",
                                    "source": "HDX",
                                    "source_description" : "The Humanitarian Data Exchange (HDX) is an open platform for sharing data across crises and organisations. Launched in July 2014, the goal of HDX is to make humanitarian data easy to find and use for analysis. The growing collection of datasets has been accessed by users in over 250 countries and territories. HDX is managed by OCHA's Centre for Humanitarian Data, which is located in The Hague, the Netherlands. OCHA is part of the United Nations Secretariat and is responsible for bringing together humanitarian actors to ensure a coherent response to emergencies. The HDX team includes OCHA staff and a number of consultants, based in North America, Europe, Africa, the Middle East and Asia.",
                                    "source_url" : "https://data.humdata.org/",
                                    "table" : resource['name'],
                                    "description" : resource["description"],
                                    ## Optional
                                    "JobType": "JSON",
                                    "CleanPush": True,
                                    "Server": "str",
                                    "UseJsonFormatForSQL": False,
                                    "CleanReplace": True,
                                    "MergeSchema": False,
                                    "tags": [{"name": each['name']} for each in data['tags']],
                                    "additional_data_sources": [{
                                        "name": data["dataset_source"],
                                    }],
                                    "limitations": limitations,
                                    "concept":  data['methodology'],
                                    "periodicity":  data['data_update_frequency'],
                                    "topic":  data['title'],
                                    "created": dt.datetime.strptime(resource['created'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S'),     #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                    "last_modified": dt.datetime.strptime(resource['last_modified'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S'),                #* ""               ""                  ""              ""
                                    "TriggerTalend" :  True,        #* initialise to True for production
                                    "SavePathForJsonOutput": ""     #* initialise as empty string for production.
                                        }
                                    }
                                
                                # TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
                                # TriggerInferShemaToJsonAPIClass.TriggerAPI()                                
                                
                                logging.info(f"{resource['format']}, {BodyDict}")
                                # logging.info(f"Conversion successful for {dataset}")
                                
                            except Exception as err:
                                
                                logging.error(f"Conversion unsuccessful for {dataset} because of {traceback.format_exc()}")
                                
                except Exception as err:
                    missedindecies.append(index)
                    logging.error(f"Couldnt save {dataset} at index {index} for this reason: {traceback.format_exc()}")
                    None
                    # logging.info(dataset, index)

            else:
                logging.info(f"Skipping {name}; no info for ESCWA Member states")
                # dataset == False

    def batch_files_download(self):
        new_arrays = np.array_split(self.datasets, 8)
        # self.get_hdx_datasets()
        self.multi_download(new_arrays[1][0:10])

    def multi_download(self,list_docs):

        logging.info(f'Starting document download: - {len(self.datasets)} in the queue')
        start = time.perf_counter()

        pool = multiprocessing.Pool()
        pool.map(self.download_file, list(list_docs))
        # pool.join()
        pool.close()


        finish = time.perf_counter()

        logging.info(f'Time consumed {finish - start} ')
    
    def clear_duplicates(self):
            # Clearing duplicate names from the Data folders vs self.datasets
        list_files_saved = os.listdir(f'{self.work_dir}')
        [self.datasets.remove(file) for file in list_files_saved if file in self.datasets]





# def old_comments():
    # def get_hdx_docs(self, list_docs=[]):
        
        
    #     if (len(list_docs) == 0 ):
            
    #         self.get_hdx_datasets()
            
    #     else:
    #         self.datasets = list_docs
            
            
            
    #     for dataset in tqdm(self.datasets):

    #         logging.info(f'Downloading {dataset}')

    #         data = self.request_data_from_hdx(dataset = dataset)
    #         filetypes = data.get_filetypes()
    #         logging.info(filetypes)
    #         if not filetypes:
    #             continue
    #         name = dataset
    #         try:
    #             logging.info(f' the current path is {os.getcwd()}')
    #             save_directory = f"{ self.work_dir }\\Data\\{name}"
    #             logging.info(f'Saving to {save_directory} \\n')
    #             os.mkdir(save_directory)
    #             # os.chdir(f"{ os.getcwd() }\\ZData\\{name}")   # initialize directory for each dataset and chdir to it
    #         except:
    #             logging.info(f"{save_directory} - Error saving the document {name}")
    #             # os.chdir(f"{save_directory} - Error")
    #         data = Dataset.read_from_hdx(name)
    #         index = self.datasets.index(name)
    #         missedindecies = []
    #         try:
    #             data.save_to_json(f"{save_directory}/metadata_{name}.json")
    #         except:
    #             missedindecies.append(index)
    #             logging.info(dataset, index)
    #             pass

    #         for resource in tqdm(data.get_resources(), ascii=True, leave=None, desc=dataset): # looping through each file associated with that specific dataset
    #             url = resource['download_url']                            # making sure naming convention is correct
    #             try:
    #                 r = requests.get(url, allow_redirects=True)
    #             except:
    #                 continue
    #             filename = resource['name']
    #             format = ('.'+resource['format'].lower())
    #             # if resource['format'] == "SQL":
    #             #         continue
    #             if format == ".geoservice" or format == ".sql":
    #                 continue
    #             if "tif" in filename or format == ".geopackage":
    #                 format = ""
    #             # elif "sql" in filename:
    #                 # continue
    #             elif format == ".shp" or format == ".emf" or format == ".geodatabase" or format == ".geotiff":
    #                 format = ".zip"
    #                 if format in filename:
    #                     format = ""
    #             elif format == ".topojson":
    #                 format = ".json"
    #                 if format in filename:
    #                     format = ""
    #             elif format in filename:
    #                 format = ""
    #             elif ".zip" in filename:
    #                 format = ""
    #             finalname = filename+format
    #             # if "sql" or "SQL" in finalname:
    #             #         continue
    #             try:
    #                 open(f"{save_directory}/{finalname}", 'wb').write(r.content)
    #                 logging.info(f'File written at {save_directory}/{finalname}')
    #             except:
    #                 continue

    # def Invalid_Char(self,df):
    #     for i in df.columns:
    #         df.rename(columns={i:re.sub('\s|,|\t|\n|;|#|{|}|\(|\)|=| |:|\\|/', '', i)}, inplace=True)
    #     return df

    # def convert_tabularfiles_Azure(self, name):

    #     logfile = open("logfile.txt", "w")

    #     file_s = os.listdir(f"Data/{name}/")
    #     logging.info(name)
    #     logging.info(file_s)
    #     for file in file_s:
    #         if ("zip" in file):
    #             logging.info('not a file to convert')
    #             continue
    #         if file.endswith('.csv'):
    #             try:
    #                 jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "utf-8-sig")).to_json(orient='records'))
    #             except:    
    #                 try:
    #                     jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "latin-1")).to_json(orient='records'))
    #                 except:
    #                     try:
    #                         jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "ISO-8859-1")).to_json(orient='records'))
    #                     except:
    #                         try:
    #                             jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "cp1252")).to_json(orient='records'))
    #                         except:
    #                             logging.info(f'Need to find appropriate encoding for {file} in {name}')
    #                             logfile.write(f"{file} in {name} \n")
    #                             # continue
    #         elif (file.endswith('.xlsx') or file.endswith('.xls')):
    #             try: 
    #                 #this is the data
    #                 jsonArray = json.loads(self.Invalid_Char(pd.read_excel(f"Data/{name}/" + file)).to_json(orient='records'))
    #             except:
    #                 try:
    #                     jsonArray = json.loads(self.Invalid_Char(pd.read_excel(f"Data/{name}/" + file, engine= 'xlrd')).to_json(orient='records'))
    #                 except:
    #                     logging.info(f'Error encoding {file} in {name}')
    #                     logfile.write(f"{file} in {name} \n")
    #                     continue
    #         else:
    #             logging.info('not a file to convert')
    #             continue
            
    #         testjson = {'JobType': 'EOSIS',
    #                     'JsonDetails': '{}',
    #                     'Data': {'data': {}, 'schema': {}},
    #                     'Server': 'str',
    #                     'Database': 'HDX',
    #                     'Table': '',
    #                     'DatabaseSourceName': 'HDX',
    #                     'Container': 'unagencies',
    #                     'CleanPush': True}
    #         testjson['Table'] = file.split('.')[0]
    #         testjson['Data']['data'] = jsonArray
    #         schema={}
    #         if jsonArray:
    #             {schema.update({key:''}) for key in testjson['Data']['data'][0].keys()}
    #         testjson['Data']['schema'] = schema
    #         metajson = open(f'Data/{name}/metadata_{name}.json')
    #         metajsonobj = json.load(metajson)
    #         tags = [item['name'] for item in metajsonobj['tags']]
    #         keystotake = {"dataset_date",
    #                     "dataset_source",
    #                     "has_geodata",
    #                     "last_modified",
    #                     "name",
    #                     "notes"
    #                     }
    #         jsondetails = {key:metajsonobj[key] for key in metajsonobj.keys() & keystotake}
    #         jsondetails['tags'] = tags
    #         testjson['JsonDetails'] = jsondetails
    #         dumps = json.dumps(testjson, indent = 4)
    #         logging.info(f'Finished converting {file}')
    #         # try:    
    #         #     os.remove(f"Data/{name}/{file.split('.')[0]}.json")
    #         # except:
    #         logging.info('Could not delete old file')
    #             # pass
    #         with open(f"Data/{name}/Azure_{file.split('.')[0]}.json", "w") as out:
    #             out.write(dumps)

    # def convert_tabularfiles_CKAN(self, name):
    #         file_s = os.listdir(f"Data/{name}/")
    #         logging.info(name)
    #         logging.info(file_s)
    #         for file in file_s:            
    #             if ("zip" in file):
    #                 logging.info('not a file to convert')
    #                 continue
    #             if file.endswith('.csv'):
    #                 try:
    #                     jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "utf-8-sig")).to_json(orient='records'))
    #                 except:    
    #                     try:
    #                         jsonArray = json.loads(self.Invalid_Char(pd.read_csv(f"Data/{name}/" + file, encoding= "latin-1")).to_json(orient='records'))
    #                     except:
    #                         logging.info(f'Error encoding {file} in {name}')
    #             elif (file.endswith('.xlsx') or file.endswith('.xls')):
    #                 try:
    #                     jsonArray=json.loads(self.Invalid_Char(pd.read_excel(f"Data/{name}/" + file)).to_json(orient='records'))
    #                 except:
    #                     try:
    #                         jsonArray = json.loads(self.Invalid_Char(pd.read_excel(f"Data/{name}/" + file, engine= 'xlrd')).to_json(orient='records'))
    #                     except:
    #                         logging.info(f'Error encoding {file} in {name}')
    #                         #logfile.write(f"{file} in {name} \n")
    #                         continue
    #             else:
    #                 logging.info('not a file to convert')
    #                 continue
                    
    #             testjson = {"data": [],
    #                         "resource_metadata": {
    #                                 "organization": {
    #                                     "ckan_id": "24569924-9027-42d0-bd86-fb4de48de570",
    #                                     "name": "un-agencies",
    #                                     "title": "UN Agencies", # UN AGENCIES ONLY NOW
    #                                     "description": "UN Agencies datasets",
    #                                     "image_url": "/base/assets/images/UN_emblem_blue.svg" 
    #                                 },
    #                                 "package": {"name": "hdx",
    #                                             "title": "HDX",
    #                                             "description": ""},
    #                                 "resource": {"name": "", # name of dataset/table
    #                                             "description": "",
    #                                             "limitations": "",
    #                                             "concept": "",
    #                                             "periodicity": "",
    #                                             "topics": "",
    #                                             "tags": ["Unclassified"],
    #                                             "sources":[]
    #                                             }
    #                         },
    #                         "sql_table_schema": {
    #                         "COLUMN_NAME": [],
    #                         "DATA_TYPE":[]
    #                         }
    #                     }
                
    #             testjson['resource_metadata']['resource']['name'] = file.split('.')[0]
    #             metajson = open(f'Data/{name}/metadata_{name}.json')
    #             metajsonobj = json.load(metajson)
    #             for resource in metajsonobj['resources']:
    #                 if bool(file == resource['name']) == True:
    #                     # logging.info('Hey')
    #                     testjson['resource_metadata']['resource']['description'] = resource['description']
    #                     #add more traits for each resource here
    #                     testjson['resource_metadata']['resource']['sources']=  [{"url": resource['download_url'].split('/download/')[0], "name" :"HDX" }]
                
    #             testjson['resource_metadata']['resource']['periodicity'] = metajsonobj['data_update_frequency']
    #             testjson['resource_metadata']['resource']['concept'] = metajsonobj['notes']       
    #             tags = [{'name':item['name']} for item in metajsonobj['tags']]
    #             testjson['resource_metadata']['resource']['tags'] = tags
    #             testjson['resource_metadata']['resource']['topics']=' , '.join([tags[i]['name'] for i in range(len(tags))])
    #             testjson['data'] = jsonArray
    #             if jsonArray:
    #                 testjson['sql_table_schema']["COLUMN_NAME"] = [key for key in testjson['data'][0].keys()]
    #                 testjson['sql_table_schema']["DATA_TYPE"] = ['nvarchar' for i in range(len(testjson['sql_table_schema']["COLUMN_NAME"]))]
                   
    #             #testjson['resource_metadata']['resource']['sources'] = metajsonobj['dataset_source']

    #             dumps = json.dumps(testjson, indent = 4)
    #             logging.info(f'Finished converting {file}')
    #             bestpracticeexample = file.split('.')[0]
    #             with open(f"Data/{name}/CKAN_{bestpracticeexample}.json", "w") as out:
    #                 out.write(dumps)
    pass
from Class import HDXScraper
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource_view import ResourceView
from hdx.data.resource import Resource
from hdx.api.locations import  Locations
import os
from tqdm import tqdm
import requests
import json
from tqdm import tqdm
import shutil
import csv
import pandas as pd

scraper = HDXScraper()

# logfile = open("logfile.txt", "w")
for name in tqdm(os.listdir('Data')[:10]):
    scraper.convert_tabularfiles_Azure(name)

# scraper.convert_tabularfiles_CKAN(name='')
    # file_s = os.listdir(f"Data/{name}/")
    # print(name)
    # print(file_s)
    # for file in file_s:
    #     if ("zip" in file):
    #         print('not a file to convert')
    #         continue
    #     if file.endswith('.csv'):
    #         try:
    #             jsonArray = json.loads(pd.read_csv(f"Data/{name}/" + file, encoding= "utf-8-sig").to_json(orient='records'))
    #         except:    
    #             try:
    #                 jsonArray = json.loads(pd.read_csv(f"Data/{name}/" + file, encoding= "latin-1").to_json(orient='records'))
    #             except:
    #                 print(f'Error encoding {file} in {name}')
    #     elif (file.endswith('.xlsx') or file.endswith('.xls')):
    #         try:
    #             jsonArray = json.loads(pd.read_excel(f"Data/{name}/" + file).to_json(orient='records'))
    #         except:
    #             try:
    #                 jsonArray = json.loads(pd.read_excel(f"Data/{name}/" + file, engine= 'xlrd').to_json(orient='records'))
    #             except:
    #                 print(f'Error encoding {file} in {name}')
    #                 logfile.write(f"{file} in {name} \n")
    #                 continue
    #     else:
    #         print('not a file to convert')
    #         continue
        
    #     testjson = {'JobType': 'EOSIS',
    #                 'JsonDetails': '{}',
    #                 'Data': {'data': {}, 'schema': {}},
    #                 'Server': 'str',
    #                 'Database': 'HDX',
    #                 'Table': '',
    #                 'DatabaseSourceName': 'HDX',
    #                 'Container': 'unagencies',
    #                 'CleanPush': True}
    #     testjson['Table'] = file.split('.')[0]
    #     testjson['Data']['data'] = jsonArray
    #     schema={}
    #     if jsonArray:
    #         {schema.update({key:''}) for key in testjson['Data']['data'][0].keys()}
    #     testjson['Data']['schema'] = schema
    #     metajson = open(f'Data/{name}/metadata_{name}.json')
    #     metajsonobj = json.load(metajson)
    #     tags = [item['name'] for item in metajsonobj['tags']]
    #     keystotake = {"dataset_date",
    #                 "dataset_source",
    #                 "has_geodata",
    #                 "last_modified",
    #                 "name",
    #                 "notes"
    #                 }
    #     jsondetails = {key:metajsonobj[key] for key in metajsonobj.keys() & keystotake}
    #     jsondetails['tags'] = tags
    #     testjson['JsonDetails'] = jsondetails
    #     dumps = json.dumps(testjson, indent = 4)
    #     print(f'Finished converting {file}')
    #     try:    
    #         os.remove(f"Data/{name}/{file.split('.')[0]}.json")
    #     except:
    #         continue    
    #     with open(f"Data/{name}/Azure_{file.split('.')[0]}.json", "w") as out:
    #         out.write(dumps)
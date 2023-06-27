from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
import logging
logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/DESA_part_2/test_file_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
try:
    BodyDict = {
        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/DESA_part_2/File 18 Annual Total Population at Mid-Year by region, subregion and country, 1950-2050 (thousands).xlsx", #* Point to downloaded data for conversion #
        "JsonDetails":{
                ## Required
                "organisation": "un-agencies",
                "source": "DESA",
                "source_description" : "Rooted in the United Nations Charter and guided by the transformative 2030 Agenda for Sustainable Development, the UN Department of Economic and Social Affairs (UN DESA) upholds the development pillar of the United Nations.",
                "source_url" : "https://www.un.org/development/desa/pd/data-landing-page",
                "table" : 'test',
                "description" : 'test', #+ indc + '\n' + note,
                ## Optional
                "JobType": "JSON",
                "CleanPush": True,
                "Server": "str",
                "UseJsonFormatForSQL":  False,
                "CleanReplace":True,
                "MergeSchema": False,
                "tags": [{
                    "name": 'test'
                }],
                "additional_data_sources": [{
                    "name": ""
                }],
                "limitations":"",
                "concept":  'test',
                "periodicity":  "",
                "topic":  'test',
                "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified": "",                #* ""               ""                  ""              ""
                "TriggerTalend" :  False,    #* initialise to True for production
                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/DESA_part_2" #* initialise as empty string for production.
            }
        }
    TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
    TriggerInferShemaToJsonAPIClass.TriggerAPI()
    logging.info(f"Conversion successful - test file ")
    print(BodyDict)
except  Exception as err:
    print(err)
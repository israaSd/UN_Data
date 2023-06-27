import requests
import traceback
from pydantic import BaseModel

class TriggerInferShemaToJsonAPI:
    def __init__(self, BodyDict):
        self.URL = "http://10.30.31.77:8010/InferSchemaAndConvertToJson"

        ## Verify Input
        class TagsClass(BaseModel):
            name: str = "Unclassified"

        class JsonDetailsModel(BaseModel):
            ## For Delta
            JobType: str = "JSON"
            CleanPush: bool = True
            Server: str = "str"
            UseJsonFormatForSQL: bool = False
            CleanReplace: bool = True
            MergeSchema: bool = False
            ## For Both Metadata
            tags: list[TagsClass] = [TagsClass()]
            organisation: str
            source: str
            source_description: str
            source_url: str
            table: str
            description: str
            limitations: str = ""
            concept: str = ""
            periodicity: str = ""
            topic: str = ""
            created: str = ""
            last_modified: str = ""

        class InferSchemaToJsonJOB(BaseModel):
            JobPath: str
            JsonDetails: JsonDetailsModel

        InferSchemaToJsonJOB(**BodyDict)

        self.BodyDict = BodyDict

    def TriggerAPI(self):
        JsonBody = self.BodyDict
        try:
            Response = requests.post(url = self.URL, json=JsonBody)
            ResponseDict = Response.json()

        except Exception as e:
            LogMessage = f"\nFailed To Connect To Infer Shema and Save To Json API due to this error: {traceback.format_exc()}"
            #print(LogMessage)
            raise ConnectionError(LogMessage)
        if not Response.ok:
            LogMessage = f"\nFailed To submit job To InferShemaSaveToJson API due to this error: {ResponseDict}"
            # print(LogMessage)
            raise ConnectionRefusedError(LogMessage)





#
# if __name__ == "__main__":
#
    # BodyDict = {

    #         "JobPath":"//10.30.31.77/data_collection_dump/RawData/Africa_COVID19_Daily_Infections_National.xlsx",
    #         "JsonDetails":{
    #                 ## Required
    #                 "organisation": "un-agencies",
    #                 "source": "str",
    #                 "source_description" : "str",
    #                 "source_url" : "str",
    #                 "table" : "str",
    #                 "description" : "str",
    #                 ## Optional
    #                 "JobType": "JSON",
    #                 "CleanPush": True,
    #                 "Server": "str",
    #                 "UseJsonFormatForSQL":  False,
    #                 "CleanReplace":True,
    #                 "MergeSchema": False,
    #                 "tags": [{
    #                     "name":"Unclassified"
    #                 }],
    #                 "limitations":"",
    #                 "concept":  "",
    #                 "periodicity":  "",
    #                 "topic":  "",
    #                 "created":"", ## this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
    #                 "last_modified": "" ## this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
    #         }
    #     }



    # TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
    # TriggerInferShemaToJsonAPIClass.TriggerAPI()
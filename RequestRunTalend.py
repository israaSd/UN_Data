import requests
import traceback

class RequestRunJobTalend:
    def __init__(self):
        self.URL = "http://10.30.31.77:8020/talendjob"


    def TriggerTalendckanJob(self):
        JsonBody = {
            "JobType":"ckan"
        }
        i=1
        Maxi = 5
        while i<=Maxi:
            try:
                Response = requests.post(url = self.URL, json=JsonBody)
                ResponseDict = Response.json()
                break
            except Exception as e:
                LogMessage = f"{i}: Failed To Connect To Talend Wrapper API due to this error: {traceback.format_exc()}"
                print(LogMessage)
            i=i+1

    def TriggerTalenddeltaJob(self):
        JsonBody = {
            "JobType":"delta"
        }
        i=1
        Maxi = 5
        while i<=Maxi:
            try:
                Response = requests.post(self.URL, json=JsonBody)
                ResponseDict = Response.json()
                break
            except Exception as e:
                LogMessage = f"{i}: Failed To Connect To Talend Wrapper API due to this error: {traceback.format_exc()}"
                print(LogMessage)
            i=i+1
    
    def TriggerBothJobs(self):
        JsonBody = {
            "JobType":"both"
        }
        i=1
        Maxi = 5
        while i<=Maxi:
            try:
                Response = requests.post(self.URL, json=JsonBody)
                ResponseDict = Response.json()
                break
            except Exception as e:
                LogMessage = f"{i}: Failed To Connect To Talend Wrapper API due to this error: {traceback.format_exc()}"
                print(LogMessage)
            i=i+1

#
# if __name__ == "__main__":
#     RequestRunJobTalendClass = RequestRunJobTalend()
#     #RequestRunJobTalendClass.TriggerTalendckanJob()
#     RequestRunJobTalendClass.TriggerTalenddeltaJob()


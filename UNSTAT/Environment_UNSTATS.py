from bs4 import BeautifulSoup
import requests
import urllib.request
import logging
import pandas as pd
from RequestInferSchemaToJsonAPI.main import TriggerInferShemaToJsonAPI
from Hashing.HashScrapedData import _hashing
base_path = '\\\\10.30.31.77\\data_collection_dump\\RawData\\UNStat_Env'
#log_path = '\\\\10.30.31.77\\data_collection_dump\\TACFolder\\ScraperLogs'

# format of the log message in specific file when download the data


def execute():
    logging.basicConfig(handlers=[logging.FileHandler(f"//10.30.31.77/data_collection_dump/TACFolder/ScraperLogs/UNStat_Env_out.log"),
                                logging.StreamHandler()], level=logging.INFO)
    # use requests to fetch the url
    url='https://unstats.un.org/unsd/envstats/qindicators'
    result = requests.get(url)
    # create soup object to parse content
    soup = BeautifulSoup(result.content,'html.parser')

    parent_node = soup.find(class_ = 'row tab-v3')
    child_node = parent_node.find_all(class_ = 'list-unstyled margin-bottom-15')
    for each in child_node:
        tag = soup.find_all("h4")[child_node.index(each)].text
        #print(tag)
        descandents = each.find_all('a')
        #print(descandents)
        for i in descandents:

            title = i.text.strip()

            BodyDict = {
                        "JobPath":f"//10.30.31.77/data_collection_dump/RawData/UNStat_Env/{title}.xlsx", #* Point to downloaded data for conversion #
                        "JsonDetails":{
                                ## Required
                                "organisation": "un-agencies",
                                "source": "UNSTATS",
                                "source_description" : "The Environment Statistics Section of the United Nations Statistics Division (UNSD) is engaged in the development of methodology, data collection, capacity development, and coordination in the fields of environmental statistics and indicators.",
                                "source_url" : "https://unstats.un.org/unsd/envstats/qindicators",
                                "table" : title,
                                "description" : title,
                                ## Optional
                                "JobType": "JSON",
                                "CleanPush": True,
                                "Server": "str",
                                "UseJsonFormatForSQL":  False,
                                "CleanReplace":True,
                                "MergeSchema": False,
                                "tags": [{
                                    "name": tag
                                }],
                                "additional_data_sources": [{
                                    "name": "",
                                    "url" : ""
                                }],
                                "limitations":"",
                                "concept":  "",
                                "periodicity":  "",
                                "topic":  tag,
                                "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                                "last_modified": "",                #* ""               ""                  ""              ""
                                "TriggerTalend" : True,    #* initialise to True for production
                                "SavePathForJsonOutput": "" #* initialise as empty string for production.
                            }
                        }

            if title.endswith('XLS') :
                # if title != 'Agricultural land XLS':
                link=i['href'].replace(' ','%20')
                
                urllib.request.urlretrieve(link,f"{base_path}\\{title}.xlsx")
                if len(pd.read_excel(rf"{base_path}\{title}.xlsx",sheet_name=None))==1:
                    dff = pd.read_excel(f'{base_path}\\{title}.xlsx')
                    index1 = dff.iloc[:,0].index[dff.iloc[:,0] == 'Definitions & Technical notes:'].tolist()
                    description1 = dff.iloc[(indx+2 for indx in index1),0].tolist()
                    description2 = dff.iloc[(indx+3 for indx in index1),0].tolist()
                    description3 = dff.iloc[(indx+4 for indx in index1),0].tolist()
                    description4 = dff.iloc[(indx+5 for indx in index1),0].tolist()
                    description = str(description1[0]) + str(description2[0]) + str(description3[0])  + str(description4[0])
                    BodyDict["JsonDetails"]["description"] = description
                    ind = dff.iloc[:,1].index[dff.iloc[:,1]== 'Country'].to_list()
                    if dff.iloc[:,0].str.contains('Data Quality: ').any():
                        index2 = dff.iloc[:,0].index[dff.iloc[:,0] == 'Data Quality: '].tolist()
                        limitations = dff.iloc[(ind+2 for ind in index2),0].tolist()
                        BodyDict["JsonDetails"]["limitations"] = limitations[0]
                    if dff.iloc[:,0].str.contains('Sources:').any():
                        index3 = dff.iloc[:,0].index[dff.iloc[:,0] == 'Sources:'].tolist()
                        source1 = dff.iloc[(indx+2 for indx in index3),0].tolist()
                        source2 = dff.iloc[(indx+3 for indx in index3),0].tolist()
                        # source3 = dff.iloc[(indx+4 for indx in index3),0].tolist()
                        # source4 = dff.iloc[(indx+5 for indx in index3),0].tolist()

                        url_source = source2[0].split(': ')[1]
                        name = source1[0]
                        # source = [' '.join(source)]
                        BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = name
                        BodyDict["JsonDetails"]["additional_data_sources"][0]["url"] = url_source
                        k = index3[0]
                        dff = dff[:k]
                        # ind = dff.iloc[:,1].index[dff.iloc[:,1]== 'Country'].to_list()
                    if len(ind)==1:
                        if title == 'Consumption of ozone-depleting substances XLS' :
                            ta = dff.iloc[ind[0]:,1:]
                            ta.iloc[0:3] = ta.iloc[0:3].fillna(method='ffill', axis=1)
                            ta.iloc[0:3] = ta.iloc[0:3].fillna('')
                            ta.columns = ta.iloc[0:3].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                            ta = ta.iloc[3:]
                            ta = ta.loc[:,~ta.columns.duplicated()].copy()
                            ta = ta.loc[:, ~ta.columns.str.contains('^Unnamed',na=False)]
                            # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                            ta = ta.loc[:, ~ta.columns.str.contains('Footnotes_',na=False)]
                            ta.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                        if title ==  'Greenhouse gas emissions by sector (percentage)  XLS':
                            ta = dff.iloc[ind[0]:,1:]
                            ta.iloc[0:2] = ta.iloc[0:2].fillna(method='ffill', axis=1)
                            ta.iloc[0:2] = ta.iloc[0:2].fillna('')
                            ta.columns = ta.iloc[0:2].apply(lambda x: ' - '.join(map(str,[y for y in x if y])), axis=0)
                            ta = ta.iloc[2:]
                            ta = ta.loc[:,~ta.columns.duplicated()].copy()
                            ta = ta.loc[:, ~ta.columns.str.contains('^Unnamed',na=False)]
                            # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                            ta = ta.loc[:, ~ta.columns.str.contains('Footnotes_',na=False)]
                            ta = ta.iloc[:,:8]
                            ta.to_excel(f'{base_path}\\{title}.xlsx',index=False)

                    elif len(ind)>1:
                        ta = dff.iloc[ind[1]:,1:]
                        column = ta.iloc[0].to_list()
                        ta.set_axis(column, axis=1,inplace=True)
                        ta = ta.iloc[1:]
                        ta.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                        df = pd.read_excel(f'{base_path}\\{title}.xlsx')
                        nan_value = float("NaN")
                        df.replace("", nan_value, inplace=True)
                        df.dropna(how='all', axis=1, inplace=True)
                        header2 = df.loc[0, :].values.tolist()
                        header2 = [str(elem).replace('nan',' ') for elem in header2]
                        header = zip(df.columns,header2)
                        df.set_axis(header, axis=1,inplace=True)
                        df = df.iloc[1:,:]
                        df.columns = [' '.join(map(str,df.columns[i])) for i in range(len(df.columns))]
                        df = df.loc[:, ~df.columns.str.contains('^Unnamed',na=False)]
                        df = df.loc[:, ~df.columns.str.contains('Footnotes',na=False)]
                        # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                        df.to_excel(f'{base_path}\\{title}.xlsx',index=False)


                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNStat_Env/{title}.xlsx"
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
                        print('error')
                        
                elif len(pd.read_excel(rf"{base_path}\{title}.xlsx",sheet_name=None))==2:
                    xls = pd.ExcelFile(f'{base_path}\\{title}.xlsx')
                    sheets = xls.sheet_names
                    df = pd.read_excel(f'{base_path}\\{title}.xlsx',sheets[1])
                    dfdesc = pd.read_excel(f'{base_path}\\{title}.xlsx',sheets[0])
                    index1 = dfdesc['Unnamed: 0'].index[dfdesc['Unnamed: 0'] == 'Definitions & Technical notes:'].tolist()
                    description = dfdesc.iloc[(indx+1 for indx in index1),0].tolist()
                    BodyDict["JsonDetails"]["description"] = description[0]
                    if dfdesc['Unnamed: 0'].str.contains('Data Quality:').any():
                        index = dfdesc['Unnamed: 0'].index[dfdesc['Unnamed: 0'] == 'Data Quality: '].tolist()
                        limitations = dfdesc.iloc[(ind+1 for ind in index),0].tolist()
                        BodyDict["JsonDetails"]["limitations"] = limitations[0]
                    if df.iloc[:,1].str.contains('Footnote').any():
                        index3 = df.iloc[:,1].index[df.iloc[:,1] == 'Footnote'].tolist()
                        k = index3[0]
                        df = df.iloc[:k]
                        nan_value = float("NaN")
                        df.replace("", nan_value, inplace=True)
                        df.dropna(how='all', axis=1, inplace=True)
                        df = df.loc[:, ~df.columns.str.contains('Footnotes',na=False)]
                        df = df.loc[:, ~df.columns.str.contains('Footnote',na=False)]
                        # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                        df = df.loc[:, ~df.columns.str.contains('^Unnamed',na=False)]
                        df.to_excel(f'{base_path}\\{title}.xlsx',index=False)
                    else:
                        nan_value = float("NaN")
                        df.replace("", nan_value, inplace=True)
                        df.dropna(how='all', axis=1, inplace=True)
                        df = df.loc[:, ~df.columns.str.contains('Footnotes',na=False)]
                        df = df.loc[:, ~df.columns.str.contains('Footnote',na=False)]
                        # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                        df.to_excel(f'{base_path}\\{title}.xlsx',index=False)

                    if 'disasters' in title:
                        header2 = df.loc[0, :].values.tolist()
                        df.columns = [df.columns[i-1] if 'Unnamed' in df.columns[i] else df.columns[i] for i in range(len(df.columns)) ]
                        df.columns = [df.columns[i-2] if 'Unnamed' in df.columns[i] else df.columns[i] for i in range(len(df.columns)) ]
                        df.columns = zip(['' if 'Unnamed' in df.columns[i] else df.columns[i] for i in range(len(df.columns)) ],header2)
                        df = df.iloc[1:]#.reset_index(drop=True)
                        df.columns = [' '.join(df.columns[i]) for i in range(len(df.columns))]
                        df.rename(columns={df.columns[0]: 'CountryID'},inplace=True)
                        df = df.loc[:, ~df.columns.str.contains('Footnotes',na=False)]
                        df = df.loc[:, ~df.columns.str.contains('Footnote',na=False)]
                        # df.drop(df.columns[df.columns.str.contains('Footnotes')], axis=1)
                        df = df.loc[:, ~df.columns.str.contains('^Unnamed',na=False)]
                        df.to_excel(f'{base_path}\\{title}.xlsx',index=False)

                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNStat_Env/{title}.xlsx"
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

            elif title == 'Proportion of population in coastal zones (LECZ)':
                link_pop=i['href']
                if link_pop.endswith('.htm'):
                    response = requests.get(link_pop)
                    soup_pop = BeautifulSoup(response.content,'html.parser')
                    content = soup_pop.find(valign='middle')
                    url_pop = content.find('a')['href']
                    url_pop = f'https://unstats.un.org/unsd/environment/{url_pop}'
                    urllib.request.urlretrieve(url_pop,f"{base_path}\\{title}.xls")
                    df = pd.read_excel(f"{base_path}\\{title}.xls")
                    index1 = df.iloc[:,0].index[df.iloc[:,0] == 'Definitions & Technical notes:'].tolist()
                    description1 = df.iloc[(indx+2 for indx in index1),0].tolist()
                    description2 = df.iloc[(indx+3 for indx in index1),0].tolist()
                    description3 = df.iloc[(indx+4 for indx in index1),0].tolist()
                    description4 = df.iloc[(indx+5 for indx in index1),0].tolist()
                    description = str(description1[0]) + str(description2[0])  + str(description3[0])  + str(description4[0])
                    index3 = df.iloc[:,0].index[df.iloc[:,0] == 'Sources:'].tolist()
                    source1 = df.iloc[(indx+2 for indx in index1),0].tolist()
                    source2 = df.iloc[(indx+3 for indx in index1),0].tolist()
                    source3 = df.iloc[(indx+4 for indx in index1),0].tolist()
                    source4 = df.iloc[(indx+5 for indx in index1),0].tolist()
                    source = str(source1[0]) + str(source2[0]) + str(source3[0]) + str(source4[0])
                    
                    BodyDict["JsonDetails"]["description"] = description
                    BodyDict["JsonDetails"]["additional_data_sources"][0]["name"] = source
                    # if dff.iloc[:,0].str.contains('Data Quality:').any():
                    index2 = df.iloc[:,0].index[df.iloc[:,0] == 'Data Quality: '].tolist()
                    limitations = df.iloc[(ind+2 for ind in index2),0].tolist()
                    BodyDict["JsonDetails"]["limitations"] = limitations[0]
                    b = df.iloc[35:,1:]
                    column = ['Country', 'Urban (%) - 1990', 'Rural (%) - 1990',' Total (%) - 1990', 'Urban (%) - 1995','Rural (%) - 1995',' Total (%) - 1995','Urban (%) - 2000','Rural (%) - 2000',' Total (%) - 2000']
                    b.set_axis(column, axis=1,inplace=True)
                    b = b.iloc[:220]
                    b = b.loc[:, ~b.columns.str.contains('Footnotes',na=False)]
                    b = b.loc[:, ~b.columns.str.contains('^Unnamed',na=False)]
                    # b.drop(b.columns[b.columns.str.contains('Footnotes')], axis=1)
                    b.to_excel(f"{base_path}\\{title}.xlsx",index = False)

                    try:
                        BodyDict["JobPath"] = f"//10.30.31.77/data_collection_dump/RawData/UNStat_Env/{title}.xlsx"
                        BodyDict["JsonDetails"]["table"] = title
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


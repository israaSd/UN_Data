import requests
from bs4 import BeautifulSoup
import wget
from py7zr import unpack_7zarchive
import shutil
import logging
import os
logging.basicConfig(handlers=[logging.FileHandler("out.log"),
                              logging.StreamHandler()], level=logging.INFO)
base_path='c:\\Users\\10235555\\Documents\\DataportalCrawlers\\UN_Data\\downloadfolder'
dumpfolder='C:\\Users\\10235555\\Documents\\DataportalCrawlers\\UN_Data\\dump_folder'
response = requests.get('https://unctadstat.unctad.org/EN/BulkDownload.html')
soup = BeautifulSoup(response.content, 'html.parser')
shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
div=soup.find("div",{"class":"doc-row clear"})
tag=div.find_all("h3")
t = []
for i in range(len(tag)):
    t.append(tag[i].text)

parent_node=soup.find_all(class_='doc-ref-list')
# print(len(parent_node))
for each in parent_node[:1]:
    datasets=each.find_all('li') #.find('a')['href']
    print(len(datasets))
    try:
       for i in datasets:
            print(i)
            links = i.find('a')['href'].replace('..', 'https://unctadstat.unctad.org')   
            print(links)
            titles = i.text.replace(':', ' -')
            print(titles)
            wget.download(links, f'{base_path}\\{titles}.7z')
            logging.info(f"\n Unzipping {titles} ...")
            shutil.unpack_archive(f"{base_path}\\{titles}.7z",f"{dumpfolder}")
    except:
        pass
    # try:
    #     os.listdir(f"{base_path}\\{titles}")
    # except Exception as err:
    #     logging.error(err)
    #     continue
    # for file in os.listdir(f"{base_path}\\{titles}"):
    #     filepath = f"{base_path}\\{titles}\\{file}"
    #     size = os.path.getsize(filepath)
    #     if size < 150000000: #this is a restriction on 150MB csvs which can become up to 1GB after json conversion - 55 files under 150mb
    #         # print(f"{file} {tag}")
    #         logging.info(f"Copying {file} - {size/1000000} MB")
    #         shutil.copy(filepath, dump_in_sharedfolder)




#parent_node=soup.find_all(class_='doc-ref-list')
#datasets=parent_node[0]
#  for i in datasets:
#    try:
#        links = i.find('a')['href'].replace('..', 'https://unctadstat.unctad.org')   
#        print(links)
#        titles = i.text.replace(':', ' -')
#        print(titles)
#        wget.download(links, f'{base_path}\\{titles}.7z')
#        logging.info(f"\n Unzipping {titles} ...")
#        shutil.unpack_archive(f"{base_path}\\{titles}.7z",f"{base_path}")
#    except:
#        pass


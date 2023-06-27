from bs4 import BeautifulSoup
import requests
import os
import wget
import time
base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\IPU"
import urllib.request
import logging
import pandas as pd
from selenium.webdriver.support.ui import Select
from re import search
# 

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
    logging.basicConfig(handlers=[logging.FileHandler("C:\\Users\\10235555\\Documents\\Dataportal Data\\IPU_out.log"),
                            logging.StreamHandler()], level=logging.INFO)
    chrome_options = webdriver.ChromeOptions()
    base_path = "\\\\10.30.31.77\\data_collection_dump\\RawData\\IPU" # local, gets current working directory
    prefs = {'download.default_directory' : base_path}#, "profile.content_settings.exceptions.automatic_downloads.*.setting" : 1}
    chrome_options.add_experimental_option('prefs', prefs)
    #chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.maximize_window()

    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    options = []
    labels_current = []   
    values_current = []
    labels_men = []
    values_men = []
    labels_women = []
    values_women = []
    labels_percen_women = []
    values_percen_women = []
    years_current = []
    years_men = []
    years_women = []
    years_percentage_women = []
    nb_current = []
    nb_men = []
    nb_women = []
    nb_percentage_women = []
    options_data = []
    arab_countries = ['Algeria', 'Bahrain', 'Comoros', 'Djibouti', 'Egypt', 'Iraq', 'Jordan', 'Kuwait', 'Lebanon', 'Libya', 'Mauritania', 'Morocco', 'Oman', 'Qatar', 'Saudi Arabia', 'Somalia' , 'Syria' , 'Tunisia', 'United Arab Emirates', 'Yemen']
    url = 'https://data.ipu.org/'
    driver.get(url)
    flex_parent = driver.find_element(By.CLASS_NAME,'cc-menu')
    flex_parent.click()                                
    div_select = flex_parent.find_element(By.CLASS_NAME,'form-item.form-type-select.form-item-country-and-chamber-select')
    select_element = div_select.find_element(By.CLASS_NAME,'chosen-enable.form-select.chosen-processed')
    driver.execute_script("arguments[0].style.display = 'block';", select_element)
    select_element.is_displayed()
    select = Select(select_element)
    option_list = select.options
    for option in option_list:
        for i in arab_countries:
            if i in option.text:
                options.append(option.text)
    for i in options:
    # time.sleep(1)
        select.select_by_visible_text(i)
        time.sleep(1)
        try:
            div = driver.find_element(By.CLASS_NAME,'chamber__info-inner')
            div_member = div.find_element(By.CLASS_NAME,'panel-pane.pane-panels-mini.pane-members.country-heading') 
            current_row = div.find_element(By.CLASS_NAME,'panel-pane.pane-entity-field.pane-node-field-current-members-number')
            try:
                his_current = current_row.find_element(By.CLASS_NAME,'field-items').find_element(By.CLASS_NAME,'info-pop-up.jquery-once-1-processed')
                his_current.click()
                time.sleep(1)
                section_current =  driver.find_element(By.ID,'field_current_members_number-historical').find_element(By.TAG_NAME,'section')
                # table_statut = section_statut.find_elements(By.TAG_NAME,'table')
                # print(table_statut.text)
                rows_current = section_current.find_elements(By.TAG_NAME,'tr')
                for row in rows_current[1:]:
                    cells_current = row.find_elements(By.TAG_NAME,'td')
                    cells_text_current = [cell.text for cell in cells_current]
                    # time.sleep(2)
                    if len(cells_text_current)>1:
                        # time.sleep(1)
                        year_current = cells_text_current[0] + ' - ' + i
                        nbs_current = cells_text_current[1] + ' - ' + i
                        years_current.append(year_current) 
                        nb_current.append(nbs_current)
                # time.sleep(2)                                 
                # div_hist_st = driver.find_element(By.CLASS_NAME,'ui-dialog.ui-widget.ui-widget-content.ui-corner-all.ui-front.ui-draggable.ui-resizable').get_attribute
                div_hist_current = driver.find_element(By.CSS_SELECTOR,'[aria-describedby="field_current_members_number-historical"]')
                close_button = div_hist_current.find_element(By.TAG_NAME,'button')
                close_button.click()
            except:
                pass
            
            label_current = current_row.find_element(By.CLASS_NAME,'field-label').text
            value_current = current_row.find_element(By.CLASS_NAME,'field-items').text
            value_current = value_current + ' - ' + i
            labels_current.append(label_current)
            values_current.append(value_current)
            time.sleep(1)
        except:
            pass 

        try:
            div = driver.find_element(By.CLASS_NAME,'chamber__info-inner')
            div_member = div.find_element(By.CLASS_NAME,'panel-pane.pane-panels-mini.pane-members.country-heading')
            men_row = div_member.find_element(By.CLASS_NAME,'panel-pane.pane-entity-field.pane-node-field-current-men-number')
            try:                                                                                      
                his_men = men_row.find_element(By.CLASS_NAME,'field-items').find_element(By.CLASS_NAME,'info-pop-up.jquery-once-1-processed')
                his_men.click()
                time.sleep(1)
                section_men =  driver.find_element(By.ID,'field_current_men_number-historical').find_element(By.TAG_NAME,'section')
                rows_men = section_men.find_elements(By.TAG_NAME,'tr')
                for row in rows_men[1:]:
                    cells_men = row.find_elements(By.TAG_NAME,'td')
                    cells_text_men = [cell.text for cell in cells_men]
                    # time.sleep(2)
                    if len(cells_text_men)>1:
                        # time.sleep(1)
                        year_men = cells_text_men[0] + ' - ' + i 
                        nbs_men= cells_text_men[1] + ' - ' + i 
                        years_men.append(year_men)
                        nb_men.append(nbs_men)
                # time.sleep(2)
                div_hist_men = driver.find_element(By.CSS_SELECTOR,'[aria-describedby="field_current_men_number-historical"]')
                close_button_men = div_hist_men.find_element(By.TAG_NAME,'button')      
                close_button_men.click() 
            except:
                pass
            label_men = men_row.find_element(By.CLASS_NAME,'field-label').text
            value_men = men_row.find_element(By.CLASS_NAME,'field-items').text
            value_men = value_men + ' - ' + i
            labels_men.append(label_men)
            values_men.append(value_men)
            time.sleep(1)
        except:
            pass

        try:
            div = driver.find_element(By.CLASS_NAME,'chamber__info-inner')
            div_member = div.find_element(By.CLASS_NAME,'panel-pane.pane-panels-mini.pane-members.country-heading')
            women_row = div_member.find_element(By.CLASS_NAME,'panel-pane.pane-entity-field.pane-node-field-current-women-number')
            try:
                his_women = women_row.find_element(By.CLASS_NAME,'field-items').find_element(By.CLASS_NAME,'info-pop-up.jquery-once-1-processed')
                his_women.click()
                time.sleep(1)
                table_women =  driver.find_element(By.ID,'field_current_women_number-historical').find_element(By.TAG_NAME,'section')
                rows_women = table_women.find_elements(By.TAG_NAME,'tr')
                for row in rows_women[1:]:
                    cells_women = row.find_elements(By.TAG_NAME,'td')
                    cells_text_women = [cell.text for cell in cells_women]
                    # time.sleep(2)
                    if len(cells_text_women)>1:
                        # time.sleep(1)
                        year_women = cells_text_women[0] + ' - ' + i 
                        nbs_women = cells_text_women[1] + ' - ' + i
                        years_women.append(year_women)
                        nb_women.append(nbs_women)
                # time.sleep(2)
                div_hist_women = driver.find_element(By.CSS_SELECTOR,'[aria-describedby="field_current_women_number-historical"]')
                close_button_women = div_hist_women.find_element(By.TAG_NAME,'button')   
                close_button_women.click() 
                time.sleep(1)
            except:
                pass
            label_women = women_row.find_element(By.CLASS_NAME,'field-label').text
            value_women = women_row.find_element(By.CLASS_NAME,'field-items').text
            value_women = value_women + ' - ' + i
            labels_women.append(label_women)
            values_women.append(value_women)
            time.sleep(1)
        except:
            pass

        try:
            div = driver.find_element(By.CLASS_NAME,'chamber__info-inner') 
            div_member = div.find_element(By.CLASS_NAME,'panel-pane.pane-panels-mini.pane-members.country-heading')
            percen_women_row = div_member.find_element(By.CLASS_NAME,'panel-pane.pane-ipu-country-node-property-pane.field')
            try:
                his_percentage_women = percen_women_row.find_element(By.CLASS_NAME,'field-items').find_element(By.CLASS_NAME,'info-pop-up.jquery-once-1-processed')
                his_percentage_women.click()
                time.sleep(1)
                table_percentage_women =  driver.find_element(By.ID,'current_women_percent-historical').find_element(By.TAG_NAME,'section')
                rows_percentage_women = table_percentage_women.find_elements(By.TAG_NAME,'tr')
                for row in rows_percentage_women[1:]:
                    cells_percentage_women = row.find_elements(By.TAG_NAME,'td')
                    cells_text_percentage_women = [cell.text for cell in cells_percentage_women]
                    # time.sleep(2)
                    if len(cells_text_percentage_women)>1:
                        # time.sleep(1)
                        year_percentage_women = cells_text_percentage_women[0] + ' - ' + i
                        nbs_percentage_women = cells_text_percentage_women[1] + ' - ' + i 
                        years_percentage_women.append(year_percentage_women)
                        nb_percentage_women.append(nbs_percentage_women)
                # time.sleep(1)
                div_hist_perc = driver.find_element(By.CSS_SELECTOR,'[aria-describedby="current_women_percent-historical"]')
                close_button_perc = div_hist_perc.find_element(By.TAG_NAME,'button')  
                close_button_perc.click() 
                time.sleep(1)
            except:
                pass
            label_percen_women = driver.find_element(By.XPATH,"//*[contains(text(), 'Percentage of women')]") #percen_women_row.find_element(By.CLASS_NAME,'field-label').text
            value_percen_women = percen_women_row.find_element(By.CLASS_NAME,'field-items').text
            value_percen_women = value_percen_women + ' - ' + i
            labels_percen_women.append(label_percen_women)
            values_percen_women.append(value_percen_women)
        except:
            pass
            # options_data.append(i)
            time.sleep(3)
        time.sleep(3)
        driver.back()
        flex_parent = driver.find_element(By.CLASS_NAME,'cc-menu')
        flex_parent.click()   
        time.sleep(2)                             
        div_select = flex_parent.find_element(By.CLASS_NAME,'form-item.form-type-select.form-item-country-and-chamber-select')
        select_element = div_select.find_element(By.CLASS_NAME,'chosen-enable.form-select.chosen-processed')
        driver.execute_script("arguments[0].style.display = 'block';", select_element)
        select_element.is_displayed()
        select = Select(select_element)

    years_m = []
    countries_m = []
    chambers_m = [] 
    for y_m in years_men:
        year_m = y_m.split(' - ')[0]
        years_m.append(year_m)
        country_m = y_m.split(' - ')[1]
        countries_m.append(country_m)
        chamber_m = y_m.split(' - ')[2]
        chambers_m.append(chamber_m)

    years_w = []
    countries_w = []
    chambers_w = [] 
    for y_w in years_women:
        year_w = y_w.split(' - ')[0]
        years_w.append(year_w)
        country_w = y_w.split(' - ')[1]
        countries_w.append(country_w)
        chamber_w = y_w.split(' - ')[2]
        chambers_w.append(chamber_w)

    years_c = []
    countries_c = []
    chambers_c = [] 
    for y_c in years_current:
        year_c = y_c.split(' - ')[0]
        years_c.append(year_c)
        country_c = y_c.split(' - ')[1]
        countries_c.append(country_c)
        chamber_c = y_c.split(' - ')[2]
        chambers_c.append(chamber_c)

    years_p = []
    countries_p = []
    chambers_p = [] 
    for y_p in years_percentage_women:
        year_p = y_p.split(' - ')[0]
        years_p.append(year_p)
        country_p = y_p.split(' - ')[1]
        countries_p.append(country_p)
        chamber_p = y_p.split(' - ')[2]
        chambers_p.append(chamber_p)

    nbs_m = []
    for n_m in nb_men:
        nb_m = n_m.split(' - ')[0]
        nbs_m.append(nb_m)

    nbs_w = []
    for n_w in nb_women:
        nb_w = n_w.split(' - ')[0]
        nbs_w.append(nb_w)

    nbs_c = []
    for n_c in nb_current:
        nb_c = n_c.split(' - ')[0]
        nbs_c.append(nb_c)

    nbs_p = []
    for n_p in nb_percentage_women:
        nb_p = n_p.split(' - ')[0]
        nbs_p.append(nb_p)
                                                                                        
    current = {'country'  : countries_c , 'year' : years_c , 'chambers' : chambers_c , 'Current number of members' : nbs_c }
    men = {'country'  : countries_m , 'year' : years_m , 'chambers' : chambers_m , 'number of men' : nbs_m}
    women = {'country'  : countries_w , 'year' : years_w , 'chambers' : chambers_w , 'number of women' : nbs_w}
    percentage_women = {'country'  : countries_p , 'year' : years_p , 'chambers' : chambers_p , 'percentage of women' : nbs_p}
    df = pd.DataFrame(men)
    df1 = pd.DataFrame(women)
    df2 = pd.DataFrame(current)
    df3 = pd.DataFrame(percentage_women)

    df_ipu1 = df3.merge(df1,how='outer', left_on=['year','country','chambers'], right_on=['year','country','chambers'],suffixes = ('_left', '_right'))
    df_ipu2 = df_ipu1.merge(df2,how='outer', left_on=['year','country','chambers'], right_on=['year','country','chambers'],suffixes = ('_left', '_right'))
    df_ipu = df_ipu2.merge(df,how='outer', left_on=['year','country','chambers'], right_on=['year','country','chambers'],suffixes = ('_left', '_right'))


    import numpy as np
    df_ipuk = df_ipu[:276]
    k = pd.value_counts(np.array(df_ipuk['country']))

    DZA = ['DZA'] * k['Algeria']
    BHR = ['BHR'] * k['Bahrain']
    COM = ['COM'] * k['Comoros']
    DJI = ['DJI'] * k['Djibouti']
    EGY = ['EGY'] * k['Egypt']
    IRQ = ['IRQ'] * k['Iraq']
    JOR = ['JOR'] * k['Jordan']
    KWT = ['KWT'] * k['Kuwait']
    LBN = ['LBN'] * k['Lebanon']
    LBY = ['LBY'] * k['Libya']
    MRT = ['MRT'] * k['Mauritania']
    MAR = ['MAR'] * k['Morocco']
    OMN = ['OMN'] * k['Oman']
    QAT = ['QAT'] * k['Qatar']
    SAU = ['SAU'] * k['Saudi Arabia']
    SOM = ['SOM'] * k['Somalia']
    SYR = ['SYR'] * k['Syrian Arab Republic']
    ARE = ['ARE'] * k['United Arab Emirates']
    YEM = ['YEM'] * k['Yemen']

    country_code = DZA + BHR + COM + DJI + EGY + IRQ + JOR + KWT + LBN + LBY + MRT + MAR + OMN + QAT + SAU +SOM + SYR + ARE + YEM
    country_code = country_code + ['TUN'] * 21 + ['BHR'] + ['COM'] + ['IRQ']*2 + ['JOR']*5 + ['KWT'] + ['LBN']*3 + ['MRT'] + ['MAR']*3 + ['SYR']*2 + ['TUN']*2 + ['YEM'] * 2 + ['SOM'] * 2 + ['YEM']  

    df_ipu.insert(1, "country code", country_code, True)
    df_ipu.rename(columns={df_ipu.columns[2]: 'date'},inplace=True)
    df_ipu.rename(columns={df_ipu.columns[3]: 'chamber name'},inplace=True)

    nbs_current_noyear = []
    countries_current_noyear = []
    chambers_current_noyear = [] 
    for nb_current_ny in values_current:
        nbs_current_ny = nb_current_ny.split(' - ')[0]
        nbs_current_noyear.append(nbs_current_ny)
        country_current_ny = nb_current_ny.split(' - ')[1]
        countries_current_noyear.append(country_current_ny)
        chamber_current_ny = nb_current_ny.split(' - ')[2]
        chambers_current_noyear.append(chamber_current_ny)

    nbs_men_noyear = []
    countries_m_noyear = []
    chambers_m_noyear = [] 
    for nb_m_ny in values_men:
        nbs_m_ny = nb_m_ny.split(' - ')[0]
        nbs_men_noyear.append(nbs_m_ny)
        country_m_ny = nb_m_ny.split(' - ')[1]
        countries_m_noyear.append(country_m_ny)
        chamber_m_ny = nb_m_ny.split(' - ')[2]
        chambers_m_noyear.append(chamber_m_ny)

    nbs_women_noyear = []
    countries_w_noyear = []
    chambers_w_noyear = [] 
    for nb_w_ny in values_women:
        nbs_w_ny = nb_w_ny.split(' - ')[0]
        nbs_women_noyear.append(nbs_w_ny)
        country_w_ny = nb_w_ny.split(' - ')[1]
        countries_w_noyear.append(country_w_ny)
        chamber_w_ny = nb_w_ny.split(' - ')[2]
        chambers_w_noyear.append(chamber_w_ny)

    nbs_percen_women_noyear = []
    countries_p_noyear = []
    chambers_p_noyear = [] 
    for nb_p_ny in values_percen_women:
        nbs_p_ny = nb_p_ny.split(' - ')[0]
        nbs_percen_women_noyear.append(nbs_p_ny)
        country_p_ny = nb_p_ny.split(' - ')[1]
        countries_p_noyear.append(country_p_ny)
        chamber_p_ny = nb_p_ny.split(' - ')[2]
        chambers_p_noyear.append(chamber_p_ny)

    current_ny = {'country'  : countries_current_noyear, 'chambers' : chambers_current_noyear , 'Current number of members' : nbs_current_noyear}
    men_ny = {'country'  : countries_m_noyear, 'chambers' : chambers_m_noyear , 'number of men' : nbs_men_noyear}
    women_ny = {'country'  : countries_w_noyear, 'chambers' : chambers_w_noyear , 'number of women' : nbs_women_noyear}
    percen_women_ny = {'country'  : countries_p_noyear, 'chambers' : chambers_p_noyear , 'percentage of women' : nbs_percen_women_noyear}

    df_ny = pd.DataFrame(men_ny)
    df1_ny = pd.DataFrame(women_ny)
    df2_ny = pd.DataFrame(current_ny)
    df3_ny = pd.DataFrame(percen_women_ny)

    df_ipu1_ny = df3_ny.merge(df1_ny,how='outer', left_on=['country','chambers'], right_on=['country','chambers'],suffixes = ('_left', '_right'))
    df_ipu2_ny = df_ipu1_ny.merge(df2_ny,how='outer', left_on=['country','chambers'], right_on=['country','chambers'],suffixes = ('_left', '_right'))
    df_ipu_ny = df_ipu2_ny.merge(df_ny,how='outer', left_on=['country','chambers'], right_on=['country','chambers'],suffixes = ('_left', '_right'))


    code_countries = ['DZA' , 'DZA' , 'BHR' , 'BHR' , 'COM' , 'DJI', 'EGY' , 'EGY' , 'IRQ' , 'JOR' , 'JOR' , 'KWT' , 'LBN' , 'LBY' , 'MRT' , 'MAR' , 'MAR' , 'OMN' , 'OMN' , ' QAT' , 'SAU' ,'SOM' , 'SOM' , 'SYR' , 'ARE' , 'YEM' , 'YEM' , 'TUN']
    df_ipu_ny.insert(1, "country code", code_countries, True)
    df_ipu_ny.insert(2, "date", ['']*28 , True)

    df_ipu_ny.rename(columns={df_ipu_ny.columns[3]: 'chamber name'},inplace=True)

    frame = [df_ipu_ny,df_ipu]
    df_ipu1_ny = pd.concat(frame,ignore_index=True)




    title = 'Women in parliament'
    tag = 'gender'
    topic = 'IPU Parline'
    df_ipu1_ny.to_csv(f'{base_path}\\{title}.csv',index=False)

    try:
        BodyDict = {
        "JobPath":f'//10.30.31.77/data_collection_dump/RawData/IPU/{title}.csv', #* Point to downloaded data for conversion 
        "JsonDetails":{
                ## Required
                "organisation": "un-agencies",
                "source": "IPU",
                "source_description" : "The IPU is the global organization of national parliaments",
                "source_url" : "https://data.ipu.org/",
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
                "topic":  topic,
                "created": "",                       #* this should follow the following formats %Y-%m-%dT%H:%M:%S" or "%Y-%m-%d"
                "last_modified": "",                #* ""               ""                  ""              ""
                "TriggerTalend" :  False,    #* initialise to True for production
                "SavePathForJsonOutput": "//10.30.31.77/data_collection_dump/TestData/IPU" #* initialise as empty string for production.
            }
        }
        TriggerInferShemaToJsonAPIClass = TriggerInferShemaToJsonAPI(BodyDict=BodyDict)
        TriggerInferShemaToJsonAPIClass.TriggerAPI()
        logging.info(f"Conversion successful - {title} ")
        print(BodyDict)
    except  Exception as err:
        print(err)

# execute()


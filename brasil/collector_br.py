import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json,rotatingIP
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.drive_utils import driveConnection,country2id
import pandas as pd 
import requests
import time
import numpy as np
from datetime import datetime
import random
import os

ip_generator = rotatingIP()
dc = driveConnection()
class scraper:

    def __init__(self,city):


        self.city = city.replace(" ","_")
        self.json_list = []
        self.time = datetime.now().strftime("%Y%m%d")
        self.n_request = 0
        self.headers = headers = {
            'authority': 'imoveis.mercadolivre.com.br',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            # Requests sorts cookies= alphabetically
            # 'cookie': '_ml_ga=GA1.3.736548453.1666122855; _ml_ga_gid=GA1.3.1090513046.1666122855; _ml_ci=736548453.1666122855; _d2id=31adc330-2bbb-4e32-be53-259358ba9cf2; _mldataSessionId=ba25f964-079d-46ef-3c23-daaac53a56af; _hjSessionUser_720738=eyJpZCI6IjhhMTZlZTcxLTMxNmUtNTMwOC1iODhkLWEyMGMxOGQ5ZDNkYiIsImNyZWF0ZWQiOjE2NjYxMjI4NTY0ODgsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjSession_720738=eyJpZCI6ImRkZDgxMmQyLTYwOWEtNDk2OS05OGM0LTg4ODk0OTBhYzJkYyIsImNyZWF0ZWQiOjE2NjYxMjI4NTY1MjQsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; LAST_SEARCH=apartamento; main_domain=; main_attributes=; categories=; last_query=apartamento; category=MLB1472; _gcl_au=1.1.1859674624.1666122874; _csrf=6l4LHB5BKcM7KS_DUPNe3CXi; c_ui-navigation=5.19.5; vis-re-search-mlb-map-visited=userhide; onboarding_cp=false; _ml_dc=1',
            'device-memory': '8',
            'downlink': '4.2',
            'dpr': '1',
            'ect': '4g',
            'encoding': 'UTF-8',
            'referer': 'https://imoveis.mercadolivre.com.br/venda/_DisplayType_M_item*location_lat:-23.64326837531012*-23.63426538045352,lon:-46.6096228763296*-46.59649078099269',
            'rtt': '200',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'viewport-width': '932',
            'x-csrf-token': 'ZEzHmw1W-kfrskHKHM_yTFdPLIzUqebH-gCU',
            'x-newrelic-id': 'XQ4OVF5VGwYCV1VXAQgHUQ==',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.cookies = {
            '_ml_ga': 'GA1.3.736548453.1666122855',
            '_ml_ga_gid': 'GA1.3.1090513046.1666122855',
            '_ml_ci': '736548453.1666122855',
            '_d2id': '31adc330-2bbb-4e32-be53-259358ba9cf2',
            '_mldataSessionId': 'ba25f964-079d-46ef-3c23-daaac53a56af',
            '_hjSessionUser_720738': 'eyJpZCI6IjhhMTZlZTcxLTMxNmUtNTMwOC1iODhkLWEyMGMxOGQ5ZDNkYiIsImNyZWF0ZWQiOjE2NjYxMjI4NTY0ODgsImV4aXN0aW5nIjpmYWxzZX0=',
            '_hjFirstSeen': '1',
            '_hjSession_720738': 'eyJpZCI6ImRkZDgxMmQyLTYwOWEtNDk2OS05OGM0LTg4ODk0OTBhYzJkYyIsImNyZWF0ZWQiOjE2NjYxMjI4NTY1MjQsImluU2FtcGxlIjpmYWxzZX0=',
            '_hjAbsoluteSessionInProgress': '0',
            'LAST_SEARCH': 'apartamento',
            'main_domain': '',
            'main_attributes': '',
            'categories': '',
            'last_query': 'apartamento',
            'category': 'MLB1472',
            '_gcl_au': '1.1.1859674624.1666122874',
            '_csrf': '6l4LHB5BKcM7KS_DUPNe3CXi',
            'c_ui-navigation': '5.19.5',
            'vis-re-search-mlb-map-visited': 'userhide',
            'onboarding_cp': 'false',
            '_ml_dc': '1',
        }


    def run_scraper(self,lon_min,lat_min,lon_max,lat_max,stepsize):

        grid_list = make_grid(sw_long=lon_min,
                              sw_lat=lat_min,
                              ne_long=lon_max,
                              ne_lat=lat_max,
                              stepsize=stepsize)
        print(f"Number of cells: {len(grid_list)}")
        for idx,cell in enumerate(grid_list):
            lon_min,lat_min,lon_max,lat_max = cell
            if(self.n_request % 50 == 0):
                if(self.n_request!=0):
                    s.save_data(local=True)
                pause_sec = random.random()*6
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            session.cookies.update(self.cookies)
            try:
                response = session.get(f'https://imoveis.mercadolivre.com.br/api/venda/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
                response_ok = response.ok
            except:
                response_ok = False
            while(not response_ok):
                print('\n\nSleeping for 3 mins...')
                time.sleep(60*3)
                self.proxies = {'http':ip_generator.get_proxy()} # change ip if sites goes down
                session.proxies.update(self.proxies)
                session.headers.update(self.headers)
                session.cookies.update(self.cookies)
                try:
                    response = session.get(f'https://imoveis.mercadolivre.com.br/api/venda/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
                    response_ok=response.ok
                except:
                    response_ok=False
            try:
                results = response.json()['results']
                self.n_request +=1
                if(len(results)==100 and stepsize>1000):
                    self.run_scraper(lon_min=lon_min,lat_min=lat_min,lat_max=lat_max,lon_max=lon_max,stepsize=stepsize/2)
                else:
                    print(f"{idx},step = {stepsize} m - Properties found in ({lat_min},{lon_min},{lat_max},{lon_max}): {len(results)}")
                    for data in results:
                        selected_data = prune_json(flatten_data(data))
                        self.json_list.append(selected_data)
                    time.sleep(random.random()*5)
            except:
                pass
        return 
    
    def save_data(self,local=False):
        
        cl = cleaner(pd.DataFrame.from_records(self.json_list))
        print(f"Shape Before Cleaning {cl.df.shape}")
        df_clean = cl.clean()
        print(f"Shape After cleaning: {df_clean.shape}")
        df_clean['scrapingTime'] = pd.to_datetime(self.time).strftime("%Y-%m-%d")
        csv_path = f"./data/{self.city}_{self.time}_{self.n_request}.csv"
        df_clean.to_csv(csv_path,index=False)
        folder_id = country2id['Brasil']
        print('Uploading csv ...')
        #dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}_{self.n_request}')
        print("Done!")
        self.json_list = []
        if not local:
            os.system(f" rm {csv_path}")
    
    def group_data(self):

        df_list = []
        folder_id = country2id['Brasil']
        for file in os.listdir("./data/"):
            if file.startswith(f"{self.city}_{self.time}"):
                df_list.append(pd.read_csv(os.path.join("./data",file)))
        csv_path = f"./data/{self.city}_{self.time}.csv"
        pd.concat(df_list).to_csv(csv_path,index=False)
        dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}')
        for file in os.listdir("./data/"):
            if file.startswith(f"{self.city}_{self.time}"):
                os.system(f""" rm {os.path.join("./data",file)}""")




class cleaner:

    def __init__(self,df):
        self.df = df

    def dorms_parser(self,x):
        try:
            return int(x.split(" ")[0])
        except:
            return np.nan

    def m2_parser(self,x):

        try:
            return float(x.split(" ")[0].replace(",","").replace("."," "))
        except:
            return np.nan

    def clean(self):

        for c in useless_columns:
            try:
                self.df = self.df.drop(columns=[c])
            except:
                pass

        df_clean = self.df.rename(columns=columns_rename).drop_duplicates(subset=['id']) #drop useless columns + rename columns + drop duplicated id
        currency_conv_dict = {} #build a dict with con rate for each currency in the df
        for currency in df_clean.currency.unique():
            currency_conv_dict[currency] = convert_to_USD(currency)        
        df_clean.room = df_clean.room.apply(lambda x: self.dorms_parser(x))
        df_clean.m2 = df_clean.m2.apply(lambda x: self.m2_parser(x))
        df_clean['priceUSD']=df_clean.apply(lambda row: currency_conv_dict[row.currency]*row.price,axis=1) #add a new field with the price in USD
        df_clean = df_clean.query('quantity==1') # avoid multiple ads : they are only 1.5% of the total
        df_clean = df_clean.drop(columns='quantity')
        df_clean = df_clean.drop_duplicates()
        return df_clean


df = pd.read_csv('./cities.csv')
cities = df.name.to_list()
lat_min_l = df.lat_min.to_list()
lat_max_l = df.lat_max.to_list()
lon_min_l = df.lon_min.to_list()
lon_max_l = df.lon_max.to_list()
dict_time = {}
for city,lat_min,lat_max,lon_min,lon_max in zip(cities,lat_min_l,lat_max_l,lon_min_l,lon_max_l):
    
    print(city,lat_max,lat_min,lon_max,lon_min)
    try:
        print_bbox(city=city,lat_max=lat_max,lat_min=lat_min,lon_max=lon_max,lon_min=lon_min)
    except:
        print("MAP not done!")
    starting_time = datetime.now()
    s = scraper(city=city)
    s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
    s.save_data(local=True)
    s.group_data()
    ending_time = datetime.now()
    dict_time[city] = ending_time-starting_time
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_brasil.csv')





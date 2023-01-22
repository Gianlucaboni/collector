import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json, rotatingIP
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.drive_utils import driveConnection,country2id
import pandas as pd 
import requests
import time
import os 
import numpy as np
import random
from datetime import datetime

ip_generator = rotatingIP()


class scraper:

    def __init__(self,lon_min,lat_min,lon_max,lat_max,city):

        self.lon_min = lon_min
        self.lat_min = lat_min
        self.lon_max = lon_max
        self.lat_max = lat_max
        self.city = city
        self.json_list = []
        self.n_request =  0
        self.proxies =  {'http':ip_generator.get_proxy()}
        self.time = datetime.now().strftime("%Y%m%d")
        self.headers = {
            'authority': 'inmuebles.mercadolibre.com.ec',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            # Requests sorts cookies= alphabetically
            # 'cookie': '_d2id=57db985c-ea3c-4c6d-9c50-1560fe00353a; _mldataSessionId=148953f2-a679-4793-115c-611fa2cd03b4; _ml_ga=GA1.3.399280416.1666125032; _ml_ga_gid=GA1.3.1335965546.1666125032; _ml_ci=399280416.1666125032; _hjSessionUser_720735=eyJpZCI6ImVjMTEwMzkzLTUxZWEtNTc0Zi04OTQ4LWM4MzJlZmViMDExNyIsImNyZWF0ZWQiOjE2NjYxMjUwMzM1NzMsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjSession_720735=eyJpZCI6IjgzNDA4NTY4LTYxNDUtNGU2NS04MjkwLTE0NTlkYzA1MzQwNCIsImNyZWF0ZWQiOjE2NjYxMjUwMzM1OTMsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _csrf=uAZQQs1peUZt2yaJZIud2aT1; main_domain=; main_attributes=; categories=; last_query=; category=MEC1459; c_ui-navigation=5.19.5; vis-re-search-mec-map-visited=userhide; _ml_dc=1',
            'device-memory': '8',
            'downlink': '5.2',
            'dpr': '1',
            'ect': '4g',
            'encoding': 'UTF-8',
            'referer': 'https://inmuebles.mercadolibre.com.ec/_DisplayType_M_item*location_lat:-0.08120571445682881*0.23327725643480826,lon:-79.55970883140645*-79.02755856285177',
            'rtt': '200',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'viewport-width': '1510',
            'x-csrf-token': '3nQLMweX-Tqrg05NSySbXngRRgd0VIQaxcAM',
            'x-newrelic-id': 'XQ4OVF5VGwcAXFVWBgEOVA==',
            'x-requested-with': 'XMLHttpRequest',
        }

        self.cookies = {
            '_d2id': '57db985c-ea3c-4c6d-9c50-1560fe00353a',
            '_mldataSessionId': '148953f2-a679-4793-115c-611fa2cd03b4',
            '_ml_ga': 'GA1.3.399280416.1666125032',
            '_ml_ga_gid': 'GA1.3.1335965546.1666125032',
            '_ml_ci': '399280416.1666125032',
            '_hjSessionUser_720735': 'eyJpZCI6ImVjMTEwMzkzLTUxZWEtNTc0Zi04OTQ4LWM4MzJlZmViMDExNyIsImNyZWF0ZWQiOjE2NjYxMjUwMzM1NzMsImV4aXN0aW5nIjpmYWxzZX0=',
            '_hjFirstSeen': '1',
            '_hjSession_720735': 'eyJpZCI6IjgzNDA4NTY4LTYxNDUtNGU2NS04MjkwLTE0NTlkYzA1MzQwNCIsImNyZWF0ZWQiOjE2NjYxMjUwMzM1OTMsImluU2FtcGxlIjpmYWxzZX0=',
            '_hjAbsoluteSessionInProgress': '0',
            '_csrf': 'uAZQQs1peUZt2yaJZIud2aT1',
            'main_domain': '',
            'main_attributes': '',
            'categories': '',
            'last_query': '',
            'category': 'MEC1459',
            'c_ui-navigation': '5.19.5',
            'vis-re-search-mec-map-visited': 'userhide',
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
            if(self.n_request % 20 == 0):
                pause_sec = random.random()*10
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            session.cookies.update(self.cookies)
            try:
                response = session.get(f'https://inmuebles.mercadolibre.com.ec/api/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
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
                    response = session.get(f'https://inmuebles.mercadolibre.com.ec/api/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
                    response_ok=response.ok
                except:
                    response_ok=False
            try:
                self.n_request +=1
                results = response.json()['results']
                if(len(results)==100 and stepsize>400):
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
        print(pd.DataFrame.from_records(self.json_list))
        df_clean = cl.clean()
        print(df_clean)
        df_clean['scrapingTime'] = pd.to_datetime(self.time).strftime("%Y-%m-%d")
        csv_path = f"./data/{self.city}_{self.time}.csv"
        df_clean.to_csv(csv_path,index=False)
        dc = driveConnection()
        folder_id = country2id['Ecuador']
        print('Uploading csv ...')
        dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}')
        print("Done!")
        if not local:
            os.system(f" rm {csv_path}")

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
    
    print(city)
    starting_time = datetime.now()
    try:
        print_bbox(city=city,lat_max=lat_max,lat_min=lat_min,lon_max=lon_max,lon_min=lon_min)
    except:
        print("MAP not done...")
    s = scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,city=city)
    s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
    s.save_data(local=True)
    ending_time = datetime.now()
    dict_time[city] = ending_time-starting_time
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_ecuador.csv')





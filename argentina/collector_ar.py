import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json,rotatingIP
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.drive_utils import driveConnection,country2id,telegramBot
import pandas as pd 
import numpy as np
import requests
import time
import os
from datetime import datetime
import random

ip_generator = rotatingIP()
#dc = driveConnection()
telegram = telegramBot()
class scraper:

    def __init__(self,lon_min,lat_min,lon_max,lat_max,city):

        self.lon_min = lon_min
        self.lat_min = lat_min
        self.lon_max = lon_max
        self.lat_max = lat_max
        self.city = city.replace(" ","_")
        self.json_list = []
        self.n_request = 0
        self.cookies = {
            '_ml_ga': 'GA1.3.107523300.1665769210',
            '_ml_ga_gid': 'GA1.3.1740161315.1665769210',
            '_ml_ci': '107523300.1665769210',
            '_ml_dc': '1',
            '_d2id': '62343032-1b24-4768-bd1c-4baf4bf0736a',
            '_mldataSessionId': 'e1ae9eea-706e-41b1-a0b3-7671f16f3260',
            '_hjSessionUser_720735': 'eyJpZCI6IjI1MmE3ZTc4LWIwNDItNTZmMS1hZTJiLWE3ZDkzNjhhNzUyZCIsImNyZWF0ZWQiOjE2NjU3NjkyMTE1MTcsImV4aXN0aW5nIjpmYWxzZX0=',
            '_hjFirstSeen': '1',
            '_hjSession_720735': 'eyJpZCI6ImVjMDlhZTg1LTZkOWUtNDVhMi04ODA5LTk5MmQ0YjNkZGNjYyIsImNyZWF0ZWQiOjE2NjU3NjkyMTE1OTQsImluU2FtcGxlIjpmYWxzZX0=',
            '_hjAbsoluteSessionInProgress': '0',
            'LAST_SEARCH': 'casa%20venta',
            'main_domain': '',
            'main_attributes': '',
            'categories': '',
            'last_query': 'casa%20venta',
            'category': 'MLA1468',
            '_gcl_au': '1.1.1894517304.1665769225',
            '_csrf': 'oxRwMjSzYIjWTsaaycP0194Q',
            'c_ui-navigation': '5.19.5',
            'vis-re-search-mla-map-visited': 'userhide',
            'onboarding_cp': 'false',
            'cookiesPreferencesNotLogged': '%7B%22categories%22%3A%7B%22advertising%22%3Atrue%7D%7D',
        }


        self.headers = {
            'authority': 'inmuebles.mercadolibre.com.ar',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            # Requests sorts cookies= alphabetically
            # 'cookie': '_ml_ga=GA1.3.107523300.1665769210; _ml_ga_gid=GA1.3.1740161315.1665769210; _ml_ci=107523300.1665769210; _ml_dc=1; _d2id=62343032-1b24-4768-bd1c-4baf4bf0736a; _mldataSessionId=e1ae9eea-706e-41b1-a0b3-7671f16f3260; _hjSessionUser_720735=eyJpZCI6IjI1MmE3ZTc4LWIwNDItNTZmMS1hZTJiLWE3ZDkzNjhhNzUyZCIsImNyZWF0ZWQiOjE2NjU3NjkyMTE1MTcsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjSession_720735=eyJpZCI6ImVjMDlhZTg1LTZkOWUtNDVhMi04ODA5LTk5MmQ0YjNkZGNjYyIsImNyZWF0ZWQiOjE2NjU3NjkyMTE1OTQsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; LAST_SEARCH=casa%20venta; main_domain=; main_attributes=; categories=; last_query=casa%20venta; category=MLA1468; _gcl_au=1.1.1894517304.1665769225; _csrf=oxRwMjSzYIjWTsaaycP0194Q; c_ui-navigation=5.19.5; vis-re-search-mla-map-visited=userhide; onboarding_cp=false; cookiesPreferencesNotLogged=%7B%22categories%22%3A%7B%22advertising%22%3Atrue%7D%7D',
            'device-memory': '8',
            'downlink': '9.25',
            'dpr': '1',
            'ect': '4g',
            'encoding': 'UTF-8',
            'referer': 'https://inmuebles.mercadolibre.com.ar/casas/venta/_DisplayType_M_item*location_lat:-34.59555207337973*-34.530807672645174,lon:-58.79490706637833*-58.66307112887833',
            'rtt': '250',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'viewport-width': '879',
            'x-csrf-token': 'cYA4Hdyy-ShjF3HgwjhMFvUvHQyKdUEpKUR4',
            'x-newrelic-id': 'XQ4OVF5VGwYCV1VXAggCUg==',
            'x-requested-with': 'XMLHttpRequest',
        }
        self.time = datetime.now().strftime("%Y%m%d")


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
                pause_sec = random.random()*2
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            session.cookies.update(self.cookies)
            try:
                response = session.get(f'https://inmuebles.mercadolibre.com.ar/api/casas/venta/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
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
                    response = session.get(f'https://inmuebles.mercadolibre.com.ar/api/casas/venta/_DisplayType_M_item*location_lat:{lat_min}*{lat_max},lon:{lon_min}*{lon_max}')
                    response_ok=response.ok
                except:
                    response_ok=False
            try:
                results = response.json()['results']
                self.n_request +=1
                if(len(results)>=100 and stepsize>=400):
                    self.run_scraper(lon_min=lon_min,lat_min=lat_min,lat_max=lat_max,lon_max=lon_max,stepsize=stepsize/2)
                else:
                    print(f"{idx},step = {stepsize} m - Properties found in ({lat_min},{lon_min},{lat_max},{lon_max}): {len(results)}")
                    for data in results:
                        selected_data = prune_json(flatten_data(data))
                        self.json_list.append(selected_data)
                    time.sleep(random.random()*2)
            except:
                pass
        return 
    
    def save_data(self,local=False):
        
        cl = cleaner(pd.DataFrame.from_records(self.json_list))
        print(pd.DataFrame.from_records(self.json_list))
        df_clean = cl.clean()
        print(df_clean)
        df_clean['scrapingTime'] = pd.to_datetime(self.time).strftime("%Y-%m-%d")
        folder_path = f"./data/{self.time}/"
        os.makedirs(folder_path,exist_ok=True)
        csv_path = os.path.join(folder_path,self.city+'.csv')
        df_clean.to_csv(csv_path,index=False)
        telegram.send_log(f"{self.city} done: {df_clean.shape[0]} ads found!")
        folder_id = country2id['Argentina']
        print('Uploading csv ...')
        #dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}')
        print("Done!")
        if not local:
            os.system(f"rm {csv_path}")

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
        df_clean['$m2'] = df_clean['priceUSD']/df_clean['m2'] 
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
    telegram.send_log(f"Starting {city.title()} - Argentina")
    starting_time = datetime.now()
    # try:
    #     print_bbox(city=city,lat_max=lat_max,lat_min=lat_min,lon_max=lon_max,lon_min=lon_min)
    # except:
    #     print("map not done!")
    s = scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,city=city)
    s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
    s.save_data(local=True)
    ending_time = datetime.now()
    dict_time[city] = ending_time-starting_time
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_argentina.csv')
    time.sleep(60*3)
with open("../logs/argentina.txt", "w") as file:
    file.write("argentina Done!")
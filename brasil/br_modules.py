import requests
from datetime import datetime
import time
import numpy as np
import random
import os
from __init__ import headers,cookies
import sys
import pandas as pd
sys.path.append('../')
from modules.utilities import  make_grid, flatten_data, prune_json
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.utilities import  rotatingIP
from modules.drive_utils import telegramBot,country2id
import json

class scraper:

    def __init__(self,city):


        self.city = city.replace(" ","_")
        self.json_list = []
        self.time = datetime.now().strftime("%Y%m%d")
        self.n_request = 0
        self.headers = headers 
        self.cookies = cookies
        self.ip_generator = rotatingIP()
        self.telegram = telegramBot()
        print(city)
        self.telegram.send_log(f"Starting {city.title()} - Brazil")


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
                    self.save_data(local=True)
                pause_sec = random.random()*10
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':self.ip_generator.get_proxy()}#change ip every 20 queries
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            session.cookies.update(self.cookies)
            try:
                curl = f'https://imoveis.mercadolivre.com.br/api/venda/_DisplayType_M_item*location_lat:{round(lat_min,15)}*{round(lat_max,15)},lon:{round(lon_min,15)}*{round(lon_max,15)}'
                print(curl)
                response = session.get(curl)
                response_ok = response.ok
            except:
                response_ok = False
            while(not response_ok):
                print('\n\nSleeping for 3 mins...')
                time.sleep(60*3)
                self.proxies = {'http':self.ip_generator.get_proxy()} # change ip if sites goes down
                session.proxies.update(self.proxies)
                session.headers.update(self.headers)
                session.cookies.update(self.cookies)
                try:
                    response = session.get(curl)
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

        df_to_save = pd.DataFrame.from_records(self.json_list)
        
        if(df_to_save.shape[0]>0): # if the csv is not empty
            cl = cleaner(df_to_save)
            print(f"Shape Before Cleaning {cl.df.shape}")
            df_clean = cl.clean()
            print(f"Shape After cleaning: {df_clean.shape}")
            df_clean['scrapingTime'] = pd.to_datetime(self.time).strftime("%Y-%m-%d")
            folder_path = f"./data/{self.time}/"
            os.makedirs(folder_path,exist_ok=True)
            csv_path = os.path.join(folder_path,self.city+'_'+str(self.n_request)+'.csv')
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
        folder_data = f"./data/{self.time}"
        for file in os.listdir(folder_data):
            if file.startswith(f"{self.city}"):
                df_list.append(pd.read_csv(os.path.join(folder_data,file)))
        csv_path = f"./data/{self.time}/{self.city}.csv"
        df_all = pd.concat(df_list)
        df_all.to_csv(csv_path,index=False)
        self.telegram.send_log(f"{self.city} done: {df_all.shape[0]} ads found!")

        #dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}')
        for file in os.listdir(folder_data): #delete small fractions
            if file.startswith(f"{self.city}_") and file != csv_path:
                os.system(f""" rm {os.path.join(folder_data,file)}""")
        return 

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
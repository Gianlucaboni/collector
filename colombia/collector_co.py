import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json,rotatingIP,get_lat_long
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
dc = driveConnection()
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
        self.headers = {
            'authority': 'api.fincaraiz.com.co',
            'accept': '*/*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            # Already added when you pass json=
            # 'content-type': 'application/json',
            'origin': 'https://www.fincaraiz.com.co',
            'referer': 'https://www.fincaraiz.com.co/',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
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
            json_data = {
                'filter': {
                    'offer': {
                        'slug': [
                            'sell',
                        ],
                    },
                    'locations': {
                        'location_point': [
                            [
                                lon_min,
                                lat_max,
                            ],
                            [
                                lon_max,
                                lat_min,
                            ],
                        ],
                    },
                },
                'fields': {
                    'exclude': [],
                    'include': [
                        'area',
                        'baths.id',
                        'baths.name',
                        'client.client_type',
                        'client.company_name',
                        'client.first_name',
                        'client.last_name',
                        'garages.name',
                        'is_new',
                        'locations.cities.name',
                        'locations.cities.slug',
                        'locations.countries.name',
                        'locations.countries.slug',
                        'locations.groups.name',
                        'locations.groups.slug',
                        'locations.groups.subgroups.name',
                        'locations.groups.subgroups.slug',
                        'locations.location_point',
                        'locations.neighbourhoods.name',
                        'locations.neighbourhoods.slug',
                        'locations.states.name',
                        'locations.states.slug',
                        'locations.view_map.slug',
                        'media.floor_plans.count',
                        'media.photos.list.id',
                        'media.photos.list.image.full_size',
                        'media.photos.list.is_main',
                        'media.videos.count',
                        'min_area',
                        'min_price',
                        'price',
                        'products.configuration.tag_name',
                        'products.label',
                        'products.name',
                        'products.slug',
                        'property_id',
                        'fr_property_id',
                        'rooms.name',
                        'title',
                        'property_type.name',
                        'offer.name',
                        'fr_parent_property_id',
                    ],
                    'limit': 100,
                    'offset': 0,
                    'ordering': [],
                    'platform': 40,
                    'with_algorithm': False,
                },
            }
            if(self.n_request % 30 == 0):
                #if(self.n_request!=0):
                #    self.save_data(local=True)
                #self.json_list = []
                pause_sec = random.random()*1
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            try:
                response = session.post('https://api.fincaraiz.com.co/document/api/1.0/listing/search', json=json_data)
                response_ok = response.ok
            except:
                response_ok = False
            while(not response_ok):
                print('\n\nSleeping for 3 mins...')
                time.sleep(60*3)
                self.proxies = {'http':ip_generator.get_proxy()} # change ip if sites goes down
                session.proxies.update(self.proxies)
                session.headers.update(self.headers)
                try:
                    response = session.post('https://api.fincaraiz.com.co/document/api/1.0/listing/search', json=json_data)
                    response_ok=response.ok
                except:
                    response_ok=False
            try:
                results = response.json()['hits']['hits'] #list of json
                self.n_request +=1
                if(len(results)>=100 and stepsize>1000):
                    self.run_scraper(lon_min=lon_min,lat_min=lat_min,lat_max=lat_max,lon_max=lon_max,stepsize=stepsize/2)
                else:
                    print(f"{idx},step = {stepsize} m - Properties found in ({lat_min},{lon_min},{lat_max},{lon_max}): {len(results)}")
                    for data in results:
                        selected_data = prune_json(flatten_data(data))
                        self.json_list.append(selected_data)
                    time.sleep(random.random())
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
        folder_id = country2id['Colombia']
        print('Uploading csv ...')
        #dc.push_csv(folder_id=folder_id,csv_path=csv_path,spreadsheet_name=f'{self.time}_{self.city}_{self.n_request}')
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
        self.df['currency'] = 'COP'
        self.df = self.df[[c for c in self.df.columns if 'photos' not in c]]
        telegram.send_log(f"Pre cleaning size: {self.df.shape}")
        try:
            df_clean = self.df.rename(columns=columns_rename).drop_duplicates(subset=['_source_listing_property_id']) #drop useless columns + rename columns + drop duplicated id
        except:
            pass
        try:
            currency_conv_dict = {} #build a dict with con rate for eacdh currency in the df
            df_clean['price']=df_clean['price'].apply(lambda x: float(x))
            for currency in df_clean.currency.unique():
                currency_conv_dict[currency] = convert_to_USD(currency)        
            df_clean['priceUSD']=df_clean.apply(lambda row: currency_conv_dict[row.currency]*row.price,axis=1) #add a new field with the price in USD
            print(df_clean.shape[0])
            df_clean = df_clean.drop_duplicates()
            df_clean['lat'],df_clean['lon'] = zip(*df_clean.coords.apply(lambda x: get_lat_long(x)))#create lat,lon from coords
            df_clean.drop(columns='coords',inplace=True) #drop coords
            print(df_clean.shape[0])
            df_clean.m2 = df_clean.m2.apply(lambda x: float(x))
            df_clean['$m2'] = df_clean['priceUSD']/df_clean['m2'] 

            print("WELL DONE")
        except Exception as e:
            print(e)

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
    except Exception as e:
        print(str(e)+"  map not done!")
    s = scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,city=city)
    telegram.send_log(f"Starting: {city}")    
    s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
    s.save_data(local=True)
    ending_time = datetime.now()
    dict_time[city] = ending_time-starting_time
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_colombia.csv')
    time.sleep(60*3)
with open("../logs/colombia.txt", "w") as file:
    file.write("Colombia Done!")

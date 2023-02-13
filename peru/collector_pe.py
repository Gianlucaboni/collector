import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json,rotatingIP
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.drive_utils import driveConnection,country2id,telegramBot
import pandas as pd 
import requests
import time
import time
import os
from datetime import datetime
import random
import numpy as np

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
            '_ga': 'GA1.3.1011112485.1669205387',
            'G_ENABLED_IDPS': 'google',
            '_fbp': 'fb.2.1669205387552.1866864617',
            '_delighted_web': '{%22mLk6P46sg0U27uB0%22:{%22_delighted_fst%22:{%22t%22:%221669205387627%22}}}',
            'g_state': '{"i_p":1669212605128,"i_l":1}',
            'PHPSESSIDIC': 'ummuo66iamcvnsvau2lak7c282',
            '_gid': 'GA1.3.706356242.1669588788',
            '_gat': '1',
            'guestID': '61_6383e78c44994',
            'G_AUTHUSER_H': '0',
        }
        self.headers = {
            'authority': 'www.infocasas.com.pe',
            'accept': '*/*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            # Requests sorts cookies= alphabetically
            # 'cookie': '_ga=GA1.3.1011112485.1669205387; G_ENABLED_IDPS=google; _fbp=fb.2.1669205387552.1866864617; _delighted_web={%22mLk6P46sg0U27uB0%22:{%22_delighted_fst%22:{%22t%22:%221669205387627%22}}}; g_state={"i_p":1669212605128,"i_l":1}; PHPSESSIDIC=ummuo66iamcvnsvau2lak7c282; _gid=GA1.3.706356242.1669588788; _gat=1; guestID=61_6383e78c44994; G_AUTHUSER_H=0',
            'origin': 'https://www.infocasas.com.pe',
            'referer': 'https://www.infocasas.com.pe/venta/casas-y-departamentos/lima',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
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
        self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
        ip = self.proxies['http'].split(":")[0]
        for idx,cell in enumerate(grid_list):
            lon_min,lat_min,lon_max,lat_max = cell
            session = requests.session()
            if(self.n_request % 20 == 0):
                pause_sec = random.random()*10
                print(f"Pause for {round(pause_sec,2)} secs...")
                time.sleep(pause_sec) # random secods of pause
                self.proxies = {'http':ip_generator.get_proxy()}#change ip every 20 queries
                ip = self.proxies['http'].split(":")[0]
            session = requests.session()
            session.proxies.update(self.proxies)
            session.headers.update(self.headers)
            session.cookies.update(self.cookies)
            print(ip)
            data = {
                'view': 'galeria',
                'mid': 'propiedades',
                'func': 'listar',
                'IDinmobiliarias': '0',
                'IDoperaciones': '1',
                'tipoPropiedad[]': [
                    '1',
                    '2',
                ],
                'IDdepartamentos': '117',
                'mod': 'propiedades',
                'searchstring': '',
                'precioMinimo': '',
                'precioMaximo': '',
                'IDmoneda': '6',
                'IP': ip,
                'latlong[]': [
                    f'{lat_max},{lon_min}',
                    f'{lat_min},{lon_max}',
                ],
            }
            try:
                response = session.post('https://www.infocasas.com.pe/?mid=propiedades&func=ajax_buscar_mapa&allowMapInfo',data=data,timeout=5)
                response_ok = response.ok
            except:
                response_ok = False
            while(not response_ok):
                print('\n\nSleep for 3 mins...')
                time.sleep(60*3)
                self.proxies = {'http':ip_generator.get_proxy()} # change ip if sites goes down
                session.proxies.update(self.proxies)
                session.headers.update(self.headers)
                data['IP'] = self.proxies['http'].split(':')[0]
                try:
                    response = session.post('https://www.infocasas.com.pe/?mid=propiedades&func=ajax_buscar_mapa&allowMapInfo',data=data,timeout=5)
                    response_ok = response.ok
                except:
                    response_ok=False
            try:
                results = response.json()
                self.n_request +=1
                if(len(results)>=400 and stepsize>400):
                    self.run_scraper(lon_min=lon_min,lat_min=lat_min,lat_max=lat_max,lon_max=lon_max,stepsize=stepsize/2)
                else:
                    print(f"{idx},step = {stepsize} m - Properties found in ({lat_min},{lon_min},{lat_max},{lon_max}): {len(results)}")
                    for data in results:
                        selected_data = prune_json(flatten_data(data))
                        self.json_list.append(selected_data)
                    time.sleep(random.random()*5)
            except Exception as e:
                print(e)
                pass
        return 
    
    def save_data(self,local=False):
        # cl = cleaner(pd.DataFrame.from_records(self.json_list))
        # print(pd.DataFrame.from_records(self.json_list))
        # df_clean = cl.clean()
        df_clean  = pd.DataFrame.from_records(self.json_list)
        print(df_clean)
        df_clean['scrapingTime'] = pd.to_datetime(self.time).strftime("%Y-%m-%d")
        folder_path = f"./data/{self.time}/"
        os.makedirs(folder_path,exist_ok=True)
        csv_path = os.path.join(folder_path,self.city+'.csv')
        df_clean.to_csv(csv_path,index=False)
        telegram.send_log(f"{self.city} done: {df_clean.shape[0]} ads found!")
        folder_id = country2id['Peru']
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
        self.df['currency'] = 'COP'
        self.df = self.df[[c for c in self.df.columns if 'photos' not in c]]
        df_clean = self.df.rename(columns=columns_rename).drop_duplicates(subset=['id']) #drop useless columns + rename columns + drop duplicated id
        currency_conv_dict = {} #build a dict with con rate for each currency in the df
        df_clean['price']=df_clean['price'].apply(lambda x: float(x))
        for currency in df_clean.currency.unique():
            currency_conv_dict[currency] = convert_to_USD(currency)        
        df_clean['priceUSD']=df_clean.apply(lambda row: currency_conv_dict[row.currency]*row.price,axis=1) #add a new field with the price in USD
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
    starting_time = datetime.now()
    try:
        print_bbox(city=city,lat_max=lat_max,lat_min=lat_min,lon_max=lon_max,lon_min=lon_min)
    except:
        print("Map not done!")
    s = scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,city=city)
    s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
    s.save_data()
    ending_time = datetime.now()
    dict_time[city] = ending_time-starting_time
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_peru.csv')
    time.sleep(60)
with open("../logs/peru.txt", "w") as file:
    file.write("Peru Done!")
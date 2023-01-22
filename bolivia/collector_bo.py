import sys
sys.path.append('../')
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json
from modules.utilities import convert_to_USD,columns_rename,useless_columns #data cleaning
from modules.drive_utils import driveConnection,country2id
from modules.utilities import print_bbox, make_grid, flatten_data, prune_json
import pandas as pd 
import numpy as np
import requests
import time
import os
from datetime import datetime
dc = driveConnection()


class scraper:

    def __init__(self,lon_min,lat_min,lon_max,lat_max,city):

        self.lon_min = lon_min
        self.lat_min = lat_min
        self.lon_max = lon_max
        self.lat_max = lat_max
        self.city = city.replace(" ","_")
        self.json_list = []
        self.time = datetime.now().strftime("%Y%m%d")

        self.headers = {
            'authority': 'graph.infocasas.com.uy',
            'accept': '*/*',
            'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
            # Already added when you pass json=
            # 'content-type': 'application/json',
            'origin': 'https://www.infocasas.com.bo',
            'referer': 'https://www.infocasas.com.bo/venta/casas-y-departamentos',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'x-origin': 'www.infocasas.com.bo',
        }

        self.json_data = [
            {
                'operationName': 'searchUrl',
                'variables': {
                    'params': {
                        'page': 1,
                        'order': 2,
                        'operation_type_id': 1,
                        'property_type_id': [
                            1,
                            2,
                        ],
                        'map_bounds': [
                            {
                                'latitude': lat_max,
                                'longitude':lon_max,
                            },
                            {
                                'latitude': lat_max,
                                'longitude': lon_min,
                            },
                            {
                                'latitude': lat_min,
                                'longitude': lon_min,
                            },
                            {
                                'latitude': lat_min,
                                'longitude':lon_max,
                            },
                            {
                                'latitude': lat_max,
                                'longitude': lon_max,
                            },
                        ],
                    },
                },
                'query': 'query searchUrl($params: SearchParamsInput!) {\n  searchUrl(params: $params) {\n    url\n    __typename\n  }\n}\n',
            },
            {
                'operationName': 'ResultsMap',
                'variables': {
                    'rows': 300,
                    'params': {
                        'page': 1,
                        'order': 2,
                        'operation_type_id': 1,
                        'property_type_id': [
                            1,
                            2,
                        ],
                        'map_bounds': [
                            {
                                'latitude': lat_max,
                                'longitude':lon_max,
                            },
                            {
                                'latitude': lat_max,
                                'longitude': lon_min,
                            },
                            {
                                'latitude': lat_min,
                                'longitude': lon_min,
                            },
                            {
                                'latitude': lat_min,
                                'longitude':lon_max,
                            },
                            {
                                'latitude': lat_max,
                                'longitude': lon_max,
                            },
                        ],
                        'allowMapInfo': True,
                    },
                    'page': 0,
                },
                'query': 'query ResultsMap($rows: Int!, $params: SearchParamsInput!, $page: Int) {\n  properties: searchListing(params: $params, first: $rows, page: $page) {\n    data {\n      __typename\n      id\n      latitude\n      longitude\n      id\n      price_variation {\n        difference\n        percentage\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        date\n        amount\n        __typename\n      }\n      price {\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        amount\n        hidePrice\n        __typename\n      }\n      commonExpenses {\n        amount\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        hidePrice\n        __typename\n      }\n      project {\n        id\n        minPrice {\n          currency {\n            id\n            name\n            rate\n            __typename\n          }\n          amount\n          hidePrice\n          __typename\n        }\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      temporal_price\n      temporal_currency {\n        id\n        name\n        rate\n        __typename\n      }\n      id\n      price_variation {\n        difference\n        percentage\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        date\n        amount\n        __typename\n      }\n      price {\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        amount\n        hidePrice\n        __typename\n      }\n      commonExpenses {\n        amount\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        hidePrice\n        __typename\n      }\n      project {\n        id\n        minPrice {\n          currency {\n            id\n            name\n            rate\n            __typename\n          }\n          amount\n          hidePrice\n          __typename\n        }\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      temporal_price\n      temporal_currency {\n        id\n        name\n        rate\n        __typename\n      }\n      neighborhood {\n        name\n        __typename\n      }\n      estate {\n        name\n        __typename\n      }\n      address\n      showAddress\n      project {\n        neighborhood {\n          name\n          __typename\n        }\n        estate {\n          name\n          __typename\n        }\n        address\n        __typename\n      }\n      project {\n        id\n        __typename\n      }\n      isExternal\n      highlight\n      created_at\n      updated_at\n      img\n      owner {\n        id\n        logo\n        name\n        inmoLink\n        inmoPropsLink\n        inmofull\n        __typename\n      }\n      created_at\n      updated_at\n      title\n      project {\n        title\n        __typename\n      }\n      description\n      project {\n        description\n        __typename\n      }\n      link\n      project {\n        id\n        link\n        __typename\n      }\n      title\n      link\n      owner {\n        id\n        name\n        has_whatsapp\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      title\n      seller {\n        id\n        name\n        masked_phone\n        __typename\n      }\n      owner {\n        id\n        name\n        masked_phone\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      img\n      image_count\n      youtube\n      id\n      title\n      link\n      isExternal\n      grouped_ids\n      pointType\n      property_type {\n        id\n        name\n        __typename\n      }\n      project {\n        id\n        id_form\n        link\n        isEspecial\n        hasCustomLanding\n        rooms\n        bedrooms\n        bathrooms\n        minM2\n        video\n        __typename\n      }\n      facilities {\n        id\n        name\n        group\n        __typename\n      }\n      bedrooms\n      rooms\n      bathrooms\n      guests\n      m2\n      seaDistanceName\n    }\n    paginatorInfo {\n      lastPage\n      firstItem\n      lastItem\n      total\n      __typename\n    }\n    __typename\n  }\n}\n',
            },
        ]


    def run_scraper(self,lon_min,lat_min,lon_max,lat_max,stepsize):

        grid_list = make_grid(sw_long=lon_min,
                              sw_lat=lat_min,
                              ne_long=lon_max,
                              ne_lat=lat_max,
                              stepsize=stepsize)
        print(f"Number of cells: {len(grid_list)}")
        for idx,cell in enumerate(grid_list):
            lon_min,lat_min,lon_max,lat_max = cell
            self.json_data = [
                {
                    'operationName': 'searchUrl',
                    'variables': {
                        'params': {
                            'page': 1,
                            'order': 2,
                            'operation_type_id': 1,
                            'property_type_id': [
                                1,
                                2,
                            ],
                            'map_bounds': [
                                {
                                    'latitude': lat_max,
                                    'longitude':lon_max,
                                },
                                {
                                    'latitude': lat_max,
                                    'longitude': lon_min,
                                },
                                {
                                    'latitude': lat_min,
                                    'longitude': lon_min,
                                },
                                {
                                    'latitude': lat_min,
                                    'longitude':lon_max,
                                },
                                {
                                    'latitude': lat_max,
                                    'longitude': lon_max,
                                },
                            ],
                        },
                    },
                    'query': 'query searchUrl($params: SearchParamsInput!) {\n  searchUrl(params: $params) {\n    url\n    __typename\n  }\n}\n',
                },
                {
                    'operationName': 'ResultsMap',
                    'variables': {
                        'rows': 300,
                        'params': {
                            'page': 1,
                            'order': 2,
                            'operation_type_id': 1,
                            'property_type_id': [
                                1,
                                2,
                            ],
                            'map_bounds': [
                                {
                                    'latitude': lat_max,
                                    'longitude':lon_max,
                                },
                                {
                                    'latitude': lat_max,
                                    'longitude': lon_min,
                                },
                                {
                                    'latitude': lat_min,
                                    'longitude': lon_min,
                                },
                                {
                                    'latitude': lat_min,
                                    'longitude':lon_max,
                                },
                                {
                                    'latitude': lat_max,
                                    'longitude': lon_max,
                                },
                            ],
                            'allowMapInfo': True,
                        },
                        'page': 0,
                    },
                    'query': 'query ResultsMap($rows: Int!, $params: SearchParamsInput!, $page: Int) {\n  properties: searchListing(params: $params, first: $rows, page: $page) {\n    data {\n      __typename\n      id\n      latitude\n      longitude\n      id\n      price_variation {\n        difference\n        percentage\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        date\n        amount\n        __typename\n      }\n      price {\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        amount\n        hidePrice\n        __typename\n      }\n      commonExpenses {\n        amount\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        hidePrice\n        __typename\n      }\n      project {\n        id\n        minPrice {\n          currency {\n            id\n            name\n            rate\n            __typename\n          }\n          amount\n          hidePrice\n          __typename\n        }\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      temporal_price\n      temporal_currency {\n        id\n        name\n        rate\n        __typename\n      }\n      id\n      price_variation {\n        difference\n        percentage\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        date\n        amount\n        __typename\n      }\n      price {\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        amount\n        hidePrice\n        __typename\n      }\n      commonExpenses {\n        amount\n        currency {\n          id\n          name\n          rate\n          __typename\n        }\n        hidePrice\n        __typename\n      }\n      project {\n        id\n        minPrice {\n          currency {\n            id\n            name\n            rate\n            __typename\n          }\n          amount\n          hidePrice\n          __typename\n        }\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      temporal_price\n      temporal_currency {\n        id\n        name\n        rate\n        __typename\n      }\n      neighborhood {\n        name\n        __typename\n      }\n      estate {\n        name\n        __typename\n      }\n      address\n      showAddress\n      project {\n        neighborhood {\n          name\n          __typename\n        }\n        estate {\n          name\n          __typename\n        }\n        address\n        __typename\n      }\n      project {\n        id\n        __typename\n      }\n      isExternal\n      highlight\n      created_at\n      updated_at\n      img\n      owner {\n        id\n        logo\n        name\n        inmoLink\n        inmoPropsLink\n        inmofull\n        __typename\n      }\n      created_at\n      updated_at\n      title\n      project {\n        title\n        __typename\n      }\n      description\n      project {\n        description\n        __typename\n      }\n      link\n      project {\n        id\n        link\n        __typename\n      }\n      title\n      link\n      owner {\n        id\n        name\n        has_whatsapp\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      title\n      seller {\n        id\n        name\n        masked_phone\n        __typename\n      }\n      owner {\n        id\n        name\n        masked_phone\n        __typename\n      }\n      operation_type {\n        id\n        name\n        __typename\n      }\n      img\n      image_count\n      youtube\n      id\n      title\n      link\n      isExternal\n      grouped_ids\n      pointType\n      property_type {\n        id\n        name\n        __typename\n      }\n      project {\n        id\n        id_form\n        link\n        isEspecial\n        hasCustomLanding\n        rooms\n        bedrooms\n        bathrooms\n        minM2\n        video\n        __typename\n      }\n      facilities {\n        id\n        name\n        group\n        __typename\n      }\n      bedrooms\n      rooms\n      bathrooms\n      guests\n      m2\n      seaDistanceName\n    }\n    paginatorInfo {\n      lastPage\n      firstItem\n      lastItem\n      total\n      __typename\n    }\n    __typename\n  }\n}\n',
                },
            ]
            session = requests.session()
            try:
                response = requests.post('https://graph.infocasas.com.uy/graphql', headers=self.headers, json=self.json_data)
                response_ok = response.ok
            except:
                response_ok = False
            while(not response_ok):
                print('\n\nSleep for 3 mins ...')
                time.sleep(60*3)
                try:
                    response = requests.post('https://graph.infocasas.com.uy/graphql', headers=self.headers, json=self.json_data)
                    response_ok=response.ok
                except:
                    response_ok=False
            try:
                results  = response.json()[1]['data']['properties']['data']
                if(len(results)==300 and stepsize>400):
                    self.run_scraper(lon_min=lon_min,lat_min=lat_min,lat_max=lat_max,lon_max=lon_max,stepsize=stepsize/2)
                else:
                    print(f"{idx},step = {stepsize} m - Properties found in ({lat_min},{lon_min},{lat_max},{lon_max}): {len(results)}")
                    for data in results:
                        selected_data = prune_json(flatten_data(data))
                        self.json_list.append(selected_data)
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
        folder_id = country2id['Bolivia']
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
        df_clean['priceUSD']=df_clean.apply(lambda row: currency_conv_dict[row.currency]*row.price,axis=1) #add a new field with the price in USD

        try:     
            df_clean.room = df_clean.room.apply(lambda x: self.dorms_parser(x))
        except:
            pass
        try:
            df_clean.m2 = df_clean.m2.apply(lambda x: self.m2_parser(x))
        except:
            pass
        try:
            df_clean = df_clean.query('quantity==1') # avoid multiple ads : they are only 1.5% of the total
        except:
            pass
        try: 
            df_clean = df_clean.drop(columns='quantity')
        except:
            pass
        df_clean = df_clean.drop_duplicates()
        return df_clean

df = pd.read_csv('./cities.csv')
cities = df.name.to_list()
lat_min_l = df.lat_min.to_list()
lat_max_l = df.lat_max.to_list()
lon_min_l = df.lon_min.to_list()
lon_max_l = df.lon_max.to_list()
dict_time = {}
print(df)
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
    pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_bolivia.csv')




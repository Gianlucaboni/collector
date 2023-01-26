import osmnx
import shapely
import pyproj
import requests
from bs4 import BeautifulSoup
import random
import contextily as cx
import matplotlib.pyplot as plt
import requests
import re
import os

columns_rename = {'descriptions_0_label':'m2',
'descriptions_1_label':'room',
'rooms':'room',
'seller_info_id':'sellerId',
'seller_info_tags_0':'sellerType',
'price_amount':'price',
'price_currency_id':'currency',
'latitude':'lat',
'longitude':'lon',
'official_store_text_text':'storeText',
'is_development':'developing',
'sub_title':'title',
'category_id':'categoryId',
'available_quantity':'quantity',
'seller_info_address_city_name':'city',
'seller_info_address_country_name':'country',
'precio':'price',
'_source_listing_price':'price',
'dormitorios':'room',
'lng':'lon',
'_source_listing_locations_location_point':'coords',
'_source_listing_area':'m2'
}

useless_columns=['price_discount_rate',
 'price_billing',
 'media_id',
 'media_label_text',
 'media_label_color',
 'media_type',
 'media_icon_id',
 'media_background',
 'official_store_text_color',
 'official_store_verbose_text_text',
 'official_store_verbose_text_color',
 'official_store_permalink',
 'official_store_tracks_melidata_track_path',
 'official_store_tracks_melidata_track_type',
 'official_store_tracks_melidata_track_event_data_official_store_id',
 'official_store_tracks_analytics_track_action',
 'official_store_tracks_analytics_track_category',
 'official_store_tracks_analytics_track_label',
 'seller_info_car_dealer',
 'seller_info_real_estate_agency',
 'seller_info_tags_1',
 'seller_info_tags_2',
 'seller_info_address_id',
 'seller_info_address_comment',
 'seller_info_address_address_line',
 'seller_info_address_country_id',
 'seller_info_address_state_id',
 'seller_info_address_state_name',
 'seller_info_address_city_id',
 'seller_info_car_dealer_logo',
 'seller_info_real_estate_agency_fantasy_name',
 'seller_info_tags_3',
 'seller_info_official_store_id',
 'seller_info_official_store_name',
 'seller_info_official_store_text',
 'seller_info_official_store_verbose_text',
 'seller_info_real_estate_agency_logo',
 'seller_info_tags_4',
 'seller_info_power_seller_status',
 'highlight_id',
 'highlight_label_text',
 'highlight_label_color',
 'highlight_label_background',
 'highlight_type',
 'highlight_background',
 'highlighted',
 'title',
 'vertical',
 'location',
 'tags_0',
 'tags_1',
 'from_text',
 'is_ad',
 'bookmarked',
 'subtitles_item_title',
 'subtitles_operation',
 'subtitles_development']

def print_bbox(city,lon_min,lat_min,lon_max,lat_max):
    try:
        ghent_img, ghent_ext = cx.bounds2img(lon_min,
                                            lat_min,
                                            lon_max,
                                            lat_max,
                                            ll=True,
                                            source=cx.providers.OpenTopoMap
                                            )
    except:
        ghent_img, ghent_ext = cx.bounds2img(lon_min,
                                    lat_min,
                                    lon_max,
                                    lat_max,
                                    ll=True,
                                    )
    f, ax = plt.subplots(1, figsize=(14, 14))
    ax.imshow(ghent_img, extent=ghent_ext)
    os.makedirs('./maps',exist_ok=True)
    f.savefig(f'./maps/{city}.png')
    return


def make_grid(sw_long,sw_lat,ne_long,ne_lat,stepsize=2000):
    
    to_proxy_transformer = pyproj.Transformer.from_crs('epsg:4326', 'epsg:3857')
    to_original_transformer = pyproj.Transformer.from_crs('epsg:3857','epsg:4326')

    # Create corners of rectangle to be transformed to a grid
    sw = shapely.geometry.Point((sw_long, sw_lat))
    ne = shapely.geometry.Point((ne_long, ne_lat))


    # Project corners to target projection
    transformed_sw = to_proxy_transformer.transform(sw.x, sw.y) # Transform NW point to 3857
    transformed_ne = to_proxy_transformer.transform(ne.x, ne.y) # .. same for SE

    # Iterate over 2D area
    gridpoints = []
    x = transformed_sw[0]
    while x < transformed_ne[0]:
        y = transformed_sw[1]
        x_next = x+stepsize
        while y < transformed_ne[1]:
            y_next = y + stepsize
            p = to_original_transformer.transform(x, y)
            p_next = to_original_transformer.transform(x_next,y_next)
            gridpoints.append(p+p_next)
            y = y_next
        x = x_next
    return gridpoints

def flatten_data(y):

    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def prune_json(dict):

    my_dict = dict.copy()
    keys = list(my_dict.keys())
    for key in keys:
        if 'pictures' in key or 'image' in key:
            my_dict.pop(key,None)
    return my_dict

class rotatingIP:

    def __init__(self):

        self.ip_list_website = 'https://free-proxy-list.net/'
        self.check_website = 'https://ipinfo.io/json'
        return 

    def get_proxy(self):
        while(True):

            r = requests.get(self.ip_list_website)
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.find('tbody')
            proxies = []
            for row in table:
                if row.find_all('td')[4].text =='elite proxy':
                    proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
                    proxies.append(proxy)
                else:
                    pass
            proxy = random.choice(proxies)
            print(f"Using {proxy}")
            return proxy

                
    def check_proxy(self,proxy):
        proxies = {'http':proxy}
        headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'}
        
        session = requests.session()
        session.proxies.update(proxies)
        session.headers.update(headers)
        try:
            r = session.get(self.check_website,timeout=5 )
            return r.ok
        except:
            return False

def convert_to_USD(ticker_from):

    soruce_converter = requests.get(f'https://currency.world/convert/{ticker_from}/USD').text
    soup =BeautifulSoup(soruce_converter,'html.parser')
    return float(soup.find('input',{'id':'amountv1'})['value'])

def get_lat_long(string):
    match = re.search(r"POINT \((-?\d+\.\d+) (-?\d+\.\d+)\)", string)
    if match:
        lat = float(match.group(1))
        lon = float(match.group(2))
        return lat, lon
    else:
        return np.nan,np.nan

import requests
import json 
import argparse
# Create an argument parser object
parser = argparse.ArgumentParser()

# Define the optional arguments with long-form and short-form options
parser.add_argument('-f1', '--field1', help='Description of field 1')
parser.add_argument('-f2', '--field2_2', help='Description of field 2')

# Parse the command-line arguments
args = parser.parse_args()

# Assign the argument values to variables
field1 = args.field1
field2 = args.field2_2

# Display the values of the variables
print("Field 1:", field1)
print("Field 2:", field2)


cookies = {
    '_ml_ga': 'GA1.3.700477517.1678457846',
    '_d2id': 'd56a0160-825a-4be8-a11a-8af1b0be6220',
    '_hjSessionUser_720738': 'eyJpZCI6ImFhMWJiYWY3LTE0NzItNTVmMC04NDQwLTQ3NDk5MDg5MmZkYSIsImNyZWF0ZWQiOjE2Nzg0NTc4NDcxMzMsImV4aXN0aW5nIjpmYWxzZX0=',
    '_gcl_au': '1.1.1038514009.1678457858',
    'cookiesPreferencesNotLogged': '%7B%22categories%22%3A%7B%22advertising%22%3Atrue%2C%22traceability%22%3Atrue%7D%7D',
    '_ga': 'GA1.1.700477517.1678457846',
    '_uetvid': '07ba7010bf4f11ed8b121d4f9f0bc6ae',
    '_ga_NDJFKMJ2PD': 'GS1.1.1678576911.3.0.1678576911.60.0.0',
    '_csrf': 'ytTbvOCmq8nAOg1qsiS6E2Vg',
    'c_ui-navigation': '5.22.8',
    '_ml_ga_gid': 'GA1.3.753811632.1685021980',
    '_ml_ci': '700477517.1678457846',
    'vis-re-search-mlb-map-visited': 'userhide',
    '_ml_dc': '1',
    '_mldataSessionId': '9112a677-b814-4f44-0ade-46cbcf485d25',
}

headers = {
    'authority': 'imoveis.mercadolivre.com.br',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'it-IT,it;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
    # 'cookie': '_ml_ga=GA1.3.700477517.1678457846; _d2id=d56a0160-825a-4be8-a11a-8af1b0be6220; _hjSessionUser_720738=eyJpZCI6ImFhMWJiYWY3LTE0NzItNTVmMC04NDQwLTQ3NDk5MDg5MmZkYSIsImNyZWF0ZWQiOjE2Nzg0NTc4NDcxMzMsImV4aXN0aW5nIjpmYWxzZX0=; _gcl_au=1.1.1038514009.1678457858; cookiesPreferencesNotLogged=%7B%22categories%22%3A%7B%22advertising%22%3Atrue%2C%22traceability%22%3Atrue%7D%7D; _ga=GA1.1.700477517.1678457846; _uetvid=07ba7010bf4f11ed8b121d4f9f0bc6ae; _ga_NDJFKMJ2PD=GS1.1.1678576911.3.0.1678576911.60.0.0; _csrf=ytTbvOCmq8nAOg1qsiS6E2Vg; c_ui-navigation=5.22.8; _ml_ga_gid=GA1.3.753811632.1685021980; _ml_ci=700477517.1678457846; vis-re-search-mlb-map-visited=userhide; _ml_dc=1; _mldataSessionId=9112a677-b814-4f44-0ade-46cbcf485d25',
    'device-memory': '8',
    'downlink': '10',
    'dpr': '1',
    'ect': '4g',
    'encoding': 'UTF-8',
    'referer': 'https://imoveis.mercadolivre.com.br/venda/_DisplayType_M_item*location_lat:-24.033148008642534*-23.45742956486878,lon:-46.6089559878522*-45.7685018862897',
    'rtt': '0',
    'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'viewport-width': '902',
    'x-csrf-token': 'QxSFUsiP-z7yle6ZSFVT39r1o7BypcknLm-E',
    'x-newrelic-id': 'XQ4OVF5VGwYCV1VXAQgHUQ==',
    'x-requested-with': 'XMLHttpRequest',
}


my_url = 'https://imoveis.mercadolivre.com.br/api/venda/_DisplayType_M_item*location_lat:-1.482989*-1.411123777270438,lon:-48.515282*-48.46765465336484'
url = 'https://imoveis.mercadolivre.com.br/api/venda/_DisplayType_M_item*location_lat:-24.033148008642534*-23.45742956486878,lon:-46.6089559878522*-45.7685018862897'
response = requests.get(
    url=url,
    cookies=cookies,
    headers=headers,
)



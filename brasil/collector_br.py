from br_modules import scraper
import pandas as pd 
from datetime import datetime
import argparse
# Create an argument parser object
parser = argparse.ArgumentParser()

# Define the optional arguments with long-form and short-form options
parser.add_argument('-csv', '--csv_file', help='csv file containing coordinates of the cities')
parser.add_argument('-o', '--output_file', help='name of text file to make at the end of the execution')

# Parse the command-line arguments
args = parser.parse_args()

# Assign the argument values to variables
csv_path = args.csv_file
output_file = args.output_file



#dc = driveConnection()
df = pd.read_csv(csv_path)
cities = df.name.to_list()
lat_min_l = df.lat_min.to_list()
lat_max_l = df.lat_max.to_list()
lon_min_l = df.lon_min.to_list()
lon_max_l = df.lon_max.to_list()
dict_time = {}
for city,lat_min,lat_max,lon_min,lon_max in zip(cities,lat_min_l,lat_max_l,lon_min_l,lon_max_l):
    try:    
        starting_time = datetime.now()
        s = scraper(city=city)
        s.run_scraper(lon_min=lon_min,lat_min=lat_min,lon_max=lon_max,lat_max=lat_max,stepsize=8000)
        s.save_data(local=True)
        s.group_data()
        ending_time = datetime.now()
        dict_time[city] = ending_time-starting_time
        pd.DataFrame.from_dict(dict_time,orient='index',columns=['time']).to_csv('timing_brasil.csv')
    except:
        pass
with open(f"../logs/{output_file}", "w") as file:
    file.write("Brazil Done!")




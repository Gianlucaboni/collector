import pandas as pd
import geopandas as gpd
import os
from shapely import wkt


path_data_folders  = './brasil/data'
for sub_folder in os.listdir(path_data_folders):
    if sub_folder.isnumeric():
        try:
            for city in ['sao_paolo','rio']:
                try:
                    df = pd.concat([pd.read_csv(f'{path_data_folders}/{sub_folder}/{city}A.csv'),
                                    pd.read_csv(f'{path_data_folders}/{sub_folder}/{city}B.csv'),
                                    pd.read_csv(f'{path_data_folders}/{sub_folder}/{city}C.csv'),
                                    pd.read_csv(f'{path_data_folders}/{sub_folder}/{city}D.csv')])
                    df.drop_duplicates(subset='id').to_csv(f'{path_data_folders}/{sub_folder}/{city}.csv',index=False)
                    os.system(f"rm {path_data_folders}/{sub_folder}/{city}A.csv")
                    os.system(f"rm {path_data_folders}/{sub_folder}/{city}B.csv")
                    os.system(f"rm {path_data_folders}/{sub_folder}/{city}C.csv")                
                    os.system(f"rm {path_data_folders}/{sub_folder}/{city}D.csv")
                except:
                    pass
        except Exception as e:
            print(e)
            print(f'Skipping folder {sub_folder}')

def concat_csv_files(csv_path, geojson_path,output_path_history):
    
    file_dict = {}
    for root, dirs, files in os.walk(csv_path):
        for file in files:
            if file.endswith(".csv"):
                file_name = file.split(".csv")[0]
                geojson_file = f'{file_name.lower()}_geometries.csv'
                geojson_file_path = os.path.join(geojson_path, geojson_file)
                if os.path.exists(geojson_file_path):
                    print(file_name)
                    file_path = os.path.join(root, file)
                    if file_name not in file_dict:
                        file_dict[file_name] = []
                    file_dict[file_name].append(pd.read_csv(file_path))


    # Concatenate all the dataframes in each list
    print(file_dict.keys())
    for file_name in file_dict:
            df = pd.concat(file_dict[file_name], axis=0, ignore_index=True)
            geojson_file = f'{file_name.lower()}_geometries.csv'
            geojson_file_path = os.path.join(geojson_path, geojson_file)
            

            gdf_geom = pd.read_csv(geojson_file_path)
            gdf_geom['geometry'] = gdf_geom['geometry'].apply(wkt.loads)
            gdf_geom = gpd.GeoDataFrame(gdf_geom, crs='epsg:4326')

            gdf = gpd.GeoDataFrame(df[['lat','lon','$m2','scrapingTime']],geometry=gpd.points_from_xy(df.lon,df.lat),crs='epsg:4326')
            gdf.drop(columns=['lat','lon'],inplace=True)
            df_history = gdf.sjoin(gdf_geom).groupby(['scrapingTime','name']).agg(ads=('$m2','count'),avgPrice=('$m2','mean')).reset_index()
            df_history.sort_values(['name','scrapingTime'],inplace=True)
            os.makedirs(output_path_history,exist_ok=True)
            df_history.to_csv(os.path.join(output_path_history,file_name+'_historicals.csv'),index=False)


for country in ['argentina','brasil','bolivia','chile','colombia','ecuador']:
    csv_path = f'./{country}/data'
    geojson_path = f'./{country}/geojson'
    output_path_history = f'./{country}/data/historicals'
    concat_csv_files(csv_path, geojson_path,output_path_history)

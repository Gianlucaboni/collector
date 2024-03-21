import pandas as pd
from tqdm import tqdm
import glob
import json
import os
from modules.connector import MySQL

class DataProcessor:
    def __init__(self, country):
        self.country = country
        self.load_config()
        self.failed_fn = []
        self.sql = MySQL(db='ads_db',credentials_files='./credentials/sql_findap.json',GCR=False)
        max_date  = self.sql.from_sql_to_pandas(f"SELECT max(scrapingTime) as max_date FROM ads_db.{country}")['max_date'].iloc[0]
        new_folders = [f for f in os.listdir(f"./{self.country}/data") if (f > max_date.replace("-","") and f.startswith("2"))]
        self.csv_list = [file for f in new_folders for file in glob.glob(f"./{self.country}/data/{f}/*.csv")]
        # self.csv_list = glob.glob(f"./{self.country}/data/2*/*.csv")

        print(self.csv_list)
        self.df_total = None
        return 
    def load_config(self):
        config_file = f"SQLconfig/countries_config.json"  # Updated path
        with open(config_file, 'r') as file:
            config = json.load(file)
            info = config.get(self.country,{})
            self.int_col = info['int_col']
            self.float_col = info['float_col']
            self.col_remap = info['col_remap']
            self.col_to_keep = info['col_to_keep']
            self.unique_fields = info['unique_fields']

    def to_integer(self, x):
        try:
            return int(x)
        except:
            return None

    def to_float(self, x):
        try:
            return float(round(x,4))
        except:
            return None

    def process_data(self):
        cleaned_dfs = []
        for fn in tqdm(self.csv_list):
            try:
                df = pd.read_csv(fn)
                if self.col_remap:
                    df = df.rename(columns=self.col_remap)

                # Get the intersection of col_to_keep and the columns in the dataframe
                cols_to_use = list(set(self.col_to_keep) & set(df.columns))

                # Create new columns with None values for any missing columns
                missing_cols = list(set(self.col_to_keep) - set(df.columns))
                for missing_col in missing_cols:
                    df[missing_col] = None

                t = df[cols_to_use].copy()

                for c in t.columns:
                    if c in self.int_col:
                        t[c] = t[c].apply(lambda x: self.to_integer(x))
                    elif c in self.float_col:
                        t[c] = t[c].apply(lambda x: self.to_float(x))
                    else:
                        t[c] = t[c].apply(lambda x: str(x))
                cleaned_dfs.append(t)
            except:
                self.failed_fn.append(fn)

            
            # Assuming sql and table_name are defined somewhere
        df = pd.concat(cleaned_dfs)
        self.df_total=df
        return 
    def push_to_table(self):

        try:
            self.sql.append_from_df(table_name=self.country, df=self.df_total, unique_fields=self.unique_fields)
        except Exception as e:
            print(e)
        return 
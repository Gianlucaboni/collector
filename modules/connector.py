# Last Updated: 1st November 2023
# SQLAlchemy Version Tested: 2.0.20

import pandas as pd
import os
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely import wkt
import json
import psycopg2
import sqlalchemy
import io 
import geopandas as gpd
import slack_sdk as slack
from datetime import datetime
from sqlalchemy import text
from typing import Union, Optional

# SlackBot Class
class SlackBot:

    def __init__(self,
                 token_file : str = './credentials/slack.json',
                 slack_channel : str = '#driveways'):
        """
        This is used to initialise an instance of the SlackBot class.

        token_file: The file containing the Slack credentials.
        slack_channel: The name of the Slack channel to send messages to.
        """
        with open(token_file) as json_file:
            params = json.load(json_file)
        self.token = params['SLACK_TOKEN']
        self.client = slack.WebClient(token = self.token)
        self.channel_name = slack_channel
        return 

    def send_log(self,text : str)->None:
        """
        The send_log() method is used to send log messages to the specified Slack channel.
        If the rate limit is hit, then no message is sent without stopping the execution of the main script.

        text: The text of the message to be sent.
        """
        try:
            self.client.chat_postMessage(channel = self.channel_name, text = text)
        except:
            print(f"Failing to send message to {self.channel_name}!")
            pass
        return 

# MyLogger Class
class MyLogger:

    def __init__(self,
                 folder : str = 'gardens/',
                 file_name : str = 'general_log.txt',
                 slack_channel : str = '#aa-scraper'):
        """
        This is used to initialise an instance of the MyLogger class.

        folder: The folder where log files will be stored.
        file_name: The name of the log file.
        slack_channel: The name of the Slack channel to send messages to.
        """
        self.file_name = os.path.join(folder, 
                                      file_name)
        try:
            os.makedirs(folder)
        except:
            pass
        self.sb = SlackBot(slack_channel = slack_channel)
        return 
    
    def write_log(self,
                  text : str,
                  Slack : bool = True):
        """
        The write_log() method is used to write log messages with a timestamp to the specified log file.

        text: The text of the message to be written.
        Slack: If Slack = True, the log message is also sent to Slack.
        """
        time_log = datetime.now().strftime('[%Y-%m-%d @ %H:%M] - ')
        text = time_log + text + '\n'
        with open(self.file_name, 'a') as f:
            f.write(text)
        if(Slack):
            self.sb.send_log(text)
        return 

# MyBucket Class
class MyBucket:
    
    def __init__(self,
                 credentials_file : str = './credentials/New AutoMotive Index-487e031dc242.json',
                 bucket_name : str = 'eu_csv'):
        """
        This is used to initialise an instance of the MyBucket class.

        file_name: The file containing the Google Cloud Service Account credentials.
        bucket_name: The name of the bucket you want to interact with.
        """
        self.storage_client = storage.Client.from_service_account_json(credentials_file)
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.get_bucket(self.bucket_name)
        print('Bucket ' + self.bucket_name + ' successfully found')

    def show_files_in_folder(self,
                             remote_folder : str):
        """
        The show_files_in_folder() method is used to return a list of files within a folder within the specified bucket.

        remote_folder: The folder within the bucket for which you want to list files.
        """ 
        file_list = list(self.bucket.list_blobs(prefix = remote_folder))
        if(not file_list):
            raise FileNotFoundError(f'Folder {remote_folder} not found in {self.bucket_name}.')
        return file_list

    def get_pandas_csv(self,
                       file_name : str,
                       download : bool = False,
                       local_path : str = './',
                       sep : str = ','):
        """
        The get_pandas_csv() method is used to download a CSV file from the specified bucket and return it as a Pandas dataframe.

        file_name: The CSV file you want to download.
        download: If download = True, the CSV file is also downloaded to a local path.
        local_path: The local path the CSV file is downloaded to if download = True.
        sep: The delimiter used in the CSV file.
        """
        file = self.bucket.blob(file_name)
        data = file.download_as_string()
        if(download):
            file.download_to_filename(os.path.join(local_path, 
                                                   file_name.split('/')[-1]))
        return pd.read_csv(io.BytesIO(data), 
                           sep = sep, 
                           on_bad_lines = 'skip')
    
    def upload_file_to_bucket(self,
                              path_file : str, 
                              destination_blob_name : str):
        """
        The upload_file_to_bucket() method is used to upload a local file to the specified bucket.

        path_file: The local path of the file you want to upload.
        destination_blob_name: The desired path and name for the file within the bucket.
        """
        file_name = path_file.split('/')[-1]
        destination_path = os.path.join(destination_blob_name, 
                                        file_name)
        blob = self.bucket.blob(destination_path)
        blob.upload_from_filename(path_file)
        print(f'File {path_file} uploaded to {destination_path}.')
        return 

    def download_files(self,
                       prefix_file : str,
                       local_path : str,
                       format : str = '.json',
                       file_target : Optional[str] = None):
        """
        The download_files() method is used to download files from the specified bucket to a local directory.

        prefix_file: The prefix to match.
        local_path: The local directory where the downloaded files will be saved.
        format: The file format to match.
        file_target: If provided, only a specific file is downloaded.
        """
        os.makedirs(local_path, 
                    exist_ok = True)
        blobs = self.bucket.list_blobs(prefix = prefix_file)
        for blob in blobs:
            file_name = blob.name.split('/')[-1]
            if(not file_target):
                # download all files with the specified format
                if(file_name.endswith(format)):
                    blob.download_to_filename(local_path + file_name)
                    print('Blob {} downloaded to {}.'.format(blob.name, 
                                                             local_path))
            else:
                # only download a specific file
                if(file_name == file_target):
                    blob.download_to_filename(local_path + file_name)
                    print('Blob {} downloaded to {}.'.format(blob.name, 
                                                             local_path))
        return 
    
    def copy_folder_to_bucket(self, 
                              local_folder_path : str):
        """
        The copy_folder_to_bucket() method is used to copy a local directory to the specified bucket.

        local_folder_path: The local directory you want to copy to the bucket.
        """
        if not os.path.isdir(local_folder_path):
            raise NotADirectoryError(f'{local_folder_path} is not a valid directory.')
        destination_blob_name = local_folder_path    
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, 
                                               file)
                remote_file_path = os.path.join(destination_blob_name , 
                                                os.path.relpath(local_file_path, local_folder_path))
                blob = self.bucket.blob(remote_file_path)
                blob.upload_from_filename(local_file_path)
                print(f'File {local_file_path} uploaded to {remote_file_path}.')

# MySQL Class
class MySQL:

    def __init__(self,
                 db : str,
                 credentials_files : str = './credentials/local_credentials.json',
                 GCR : bool = True):
        """
        This is used to initialise an instance of the MySQL class.

        db: The name of the MySQL database you want to interact with.
        credentials_files: The file containing the MySQL database credentials.
        """
        with open(credentials_files) as json_file:
            params = json.load(json_file)
        self.db_user = params['user']
        self.db_pass = params['password']
        self.db_name = db
        self.db_host = params['host']
        self.cloud_sql_connection_region = params['cloud_sql_connection_region']
        self.cloud_sql_connection_name = params['cloud_sql_connection_name']
        self.GCR = GCR
        try:
            self.db_connection()
            print(f'Connected to the {self.db_name} database at {self.db_host}!\nDeployment in GCR: {self.GCR}\n')
        except Exception as e:
            print(f'Impossible to connect to the {self.db_name} database at {self.db_host}.\nERROR: {e}')
        return 

    def db_connection(self):
        """
        The db_connection() method is used to establish a connection to the specified MySQL database.
        """
        if(not self.GCR):
            engine = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername = 'mysql+pymysql',
                    username = self.db_user,  # e.g. 'my-database-user'
                    password = self.db_pass,  # e.g. 'my-database-password'
                    database = self.db_name,  # e.g. 'my-database-name'
                    host = self.db_host
                )
            )
        else:
            engine = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername = 'mysql+pymysql',
                    username = self.db_user,  # e.g. 'my-database-user'
                    password = self.db_pass,  # e.g. 'my-database-password'
                    database = self.db_name,  # e.g. 'my-database-name'
                    host = self.db_host,
                    query = {
                        'unix_socket' : '{}/{}'.format('/cloudsql',  # e.g. '/cloudsql'
                                                       f'rugged-baton-283921:{self.cloud_sql_connection_region}:{self.cloud_sql_connection_name}')  # you need to specify the region and instance name in GCP
                    }
                )
            )
        return engine.connect()

    def show_db(self):
        """
        The show_db() method returns a list of all databases on the MySQL server as a Pandas dataframe.
        """
        return self.from_sql_to_pandas(sql_query = """SHOW DATABASES;""")
    
    def show_tables(self,
                    db : Optional[str] = None):
        """
        The show_tables() method returns information about the tables within the specified MySQL database as a Pandas dataframe.

        db: If provided, the name of the database for which table information is to be retrieved.
        """
        if(not db):
            db = self.db_name
        print(db)
        query_columns = f"""
                        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS, CREATE_TIME, UPDATE_TIME 
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{db}'
                        ORDER BY UPDATE_TIME;
                        """
        return self.from_sql_to_pandas(sql_query = query_columns)

    def show_columns(self,
                     table : str):
        """
        The show_columns() method returns information about a table within the specified MySQL database.

        table: The name of the table for which information is to be retrieved.
        """
        query = f"""
                SHOW COLUMNS 
                FROM {table};
                """
        return self.from_sql_to_pandas(query)

    def run_query(self,
                  query : str):
        """
        The run_query() method is used to execute a query on the specified MySQL database.
        Result is returned as a list of tuples.

        query: The query to be executed.
        """
        query = text(query)
        with self.db_connection() as conn:
            with conn.begin():
                res = conn.execute(query)
        try:
            return res.fetchall()
        except Exception as e:
            print('Nothing to return!')
            return 
    
    def db_create_table_from_csv(self,
                                 table_name : str = 'mlResults',
                                 file_path : str = f'../analysis/Wakefield/properties_Wakefield.csv',
                                 columns : Union[list, bool] = False,
                                 rename_columns_dict : Union[dict, bool] = False):
        """
        The db_create_table_from_csv() method is used to create a new table in the specified MySQL database from a CSV file.

        table_name: The name of the table to be created.
        file_path: The path to the CSV file containing the data.
        columns: A list of column names to include in the table. All columns are included by default (columns = False).
        rename_columns_dict: A dictionary to rename columns. No columns are renamed by default (rename_columns_dict = False).
        """
        csv_df = pd.read_csv(file_path)
        if(rename_columns_dict):
            csv_df = csv_df.rename(columns = rename_columns_dict)
        if(columns):
            csv_df = csv_df[columns]
        print(f'Creating {table_name} from {file_path}...\n\n')
        try: 
            with self.db_connection() as conn:
                with conn.begin():
                    csv_df.to_sql(table_name, 
                                  con = conn, 
                                  if_exists = 'fail', 
                                  chunksize = 1000, 
                                  index = False)
                    return True
        except ValueError:
            print(f'ERROR: {table_name} already exists! Create a new table or use the db_update_table_from_csv() method.')
            return False

    def db_update_table_from_csv(self,
                                 table_name : str,
                                 file_path : str,
                                 create_first : bool = True,
                                 columns : Union[list, bool] = False,
                                 rename_columns_dict : Union[dict, bool] = False):
        """
        The db_update_table_from_csv() method is used to update a table in the specified MySQL database with data from a CSV file.

        table_name: The name of the table to be updated.
        file_path: The path to the CSV file containing the data.
        create_first: If create_first = True, the table will be created from scratch.
        columns: A list of column names to include in the updated table. All columns are included by default (columns = False).
        rename_columns_dict: A dictionary to rename columns. No columns are renamed by default (rename_columns_dict = False).
        """
        if(create_first):
            if(self.db_create_table_from_csv(table_name = table_name,
                                             file_path = file_path,
                                             columns = columns,
                                             rename_columns_dict = rename_columns_dict)):
                return
        print(f'Updating {table_name}. Importing {file_path}...\n\n')
        df_updates = pd.read_csv(file_path)
        if(rename_columns_dict):
            df_updates = df_updates.rename(columns = rename_columns_dict)
        if(columns):
            df_updates = df_updates[columns]
        with self.db_connection() as conn:
            with conn.begin():
                df_updates.to_sql(f'{table_name}', 
                                  conn, 
                                  if_exists = 'append', 
                                  chunksize = 1000, 
                                  index = False)
                return

    def from_sql_to_pandas(self,
                           sql_query : str = "SELECT * FROM mlResults LIMIT 20;",
                           geometry : Union[str, bool] = False):
        """
        The from_sql_to_pandas() method is used to execute a query on the specified MySQL database.
        Result is returned as a Pandas dataframe.

        sql_query: The query to be executed.
        geometry: If provided, the name of the geometry column to handle using GeoPandas.
        """
        sql_df = pd.read_sql(sql = text(sql_query), 
                             con = self.db_connection())
        if(geometry):
            sql_df[geometry] = gpd.GeoSeries.from_wkt(sql_df[geometry]) 
            sql_df = gpd.GeoDataFrame(sql_df, 
                                      geometry = geometry)
        return sql_df

    def append_from_df(self,
                       table_name : str,
                       df : pd.DataFrame,
                       unique_fields : Optional[list] = None):
        """
        The append_from_df() method is used to append rows to a table within the specified MySQL database.

        table_name: The name of the target table.
        df: The dataframe containing the data to append.
        unique_fields: A list of columns that should have unique values.

        Rows are appended in one of two ways:
        1). If the table does not exist, the method creates a new table and loads data into it.
        2). If the table exists, the method appends data to the existing table.
        If `unique_fields` is specified, rows with existing values in these fields will not be appended.
        """
        # check if the table exists
        table_exists = table_name in self.show_tables().TABLE_NAME.to_list()
        # if the table exists, append to the existing table
        if table_exists:
            print(f'Table {table_name} exists. Appending...')
            if_exists = 'append'
            if unique_fields is not None:
                print('Ignoring duplicates...')
                columns = ','.join(unique_fields)
                # remove the temporary table if it already exists 
                try:
                    self.run_query(f"""DROP TABLE `{table_name}_to_append`""")
                except:
                    pass
                # create a new temporary table storing the data to append
                self.append_from_df(table_name = f'{table_name}_to_append', 
                                    df = df)
                print("ok")
                join_conditions = ' AND '.join([f't1.{col} = t2.{col}' for col in columns.split(',')])
                where_conditions = ' IS NULL AND '.join([f't2.{col}' for col in columns.split(',')]) + " IS NULL"
                query = f"""
                        WITH t2 AS(
                            SELECT DISTINCT {columns}
                            FROM `{table_name}`
                        )
                        SELECT t1.*
                        FROM `{table_name}_to_append` AS t1
                        LEFT JOIN t2 
                        ON {join_conditions}
                        WHERE {where_conditions};
                        """
                df = self.from_sql_to_pandas(query)
                # remove the temporary table 
                self.run_query(f"""DROP TABLE `{table_name}_to_append`""")
                print(f'{len(df)} rows to append.')
        # else, create a new table
        else:
            print(f'Table {table_name} does not exist. Creating a new one...')
            if_exists = 'fail'

        # load the dataframe 
        try:
            with self.db_connection() as conn:
                with conn.begin():
                    df.to_sql(f'{table_name}', 
                              conn, 
                              if_exists = if_exists, 
                              chunksize = 1000, 
                              index = False)
            return
        except Exception as e:
            print(e)
            print(f'ERROR: Cannot append rows to {table_name}.')

    def upsert(self, 
               table_name : str,
               df : pd.DataFrame,
               columns_to_match : list):
        """
        The upsert() method is used to update existing rows or insert new rows into a table within the specified MySQL database.
        If a matching record is found, its values are updated.
        If no matching record is found, a new record is inserted.
        
        table_name: The name of the target table.
        df: The dataframe containing the data to upsert.
        columns_to_match: A list of columns used to match records in the target table.
        """
        # check the list of columns is valid
        df_target_info = self.show_columns(table = table_name)
        
        # if the target table does not exist, create it
        if(df_target_info.shape[0] == 0):
            print(f'{table_name} does not exist. Proceeding with the creation of the table...\n\n')
            self.append_from_df(table_name = table_name, 
                                df = df) 
            return 
        if(all(elem in df_target_info.Field.values for elem in columns_to_match)):
            print(f'{table_name} exists. Updating the table...\n\n')
            print(f'Creating temp table {table_name}_new_records...')
            # remove the temporary table if it already exists 
            try:
                self.run_query(f"""DROP TABLE `{table_name}_new_records`""")
            except:
                pass
            # create a new temporary table storing the data to upsert
            self.append_from_df(table_name = f'{table_name}_new_records', 
                                df = df)
            on_statement = ' AND '.join([f'target.{col} = source.{col}' for col in columns_to_match])
            columns = df.columns.to_list()
            set_columns = [col for col in columns if col not in columns_to_match]
            set_statement = ' AND '.join([f'target.{col} = source.{col}' for col in set_columns])
            insert_statement = '(' + ', '.join([f'{col}' for col in columns]) + ')'
            values_statement = ', '.join([f'source.{col}' for col in columns])
            where_conditions = ' IS NULL AND '.join([f'target.{col}' for col in columns_to_match]) + " IS NULL"
            update_query = f"""
                           UPDATE `{table_name}` AS target
                           JOIN `{table_name}_new_records` AS source
                           ON {on_statement}
                           SET {set_statement};
                           """
            insert_query = f"""
                           INSERT INTO {table_name} {insert_statement} 
                           SELECT {values_statement}
                           FROM `{table_name}_new_records` AS source
                           LEFT JOIN {table_name} AS target 
                           ON {on_statement}
                           WHERE {where_conditions};
                           """
            # perform the update
            self.run_query(update_query)
            # perform the insert
            self.run_query(insert_query)
            # remove the temporary table
            self.run_query(f"""DROP TABLE `{table_name}_new_records`""")
            return
        else:
            print(f"'columns_to_match' must be a list of valid columns in {table_name}.")
            return

# MyPostgres Class
class MyPostgres:
    
    def __init__(self,
                 credentials_file : str = './credentials/driveways-postgres.json'):
        """
        This is used to initialise an instance of the MyPostgres class.

        credentials_files: The file containing the PostgreSQL database credentials.
        """
        with open(credentials_file) as json_file:
                params = json.load(json_file)
        self.db_user = params['user']
        self.db_pass = params['password']
        self.db_name = params['database']
        self.db_host = params['host']
        self.con = psycopg2.connect(host = self.db_host,
                                    database = self.db_name,
                                    user = self.db_user,
                                    password = self.db_pass)
        return 

    def show_tables(self):
        """
        The show_tables() method returns information about the tables within the specified PostgreSQL database as a Pandas dataframe.
        """
        con = self.con
        cur = con.cursor()
        cur.execute("""
                    SELECT relname 
                    FROM pg_class 
                    WHERE relkind = 'r' and relname !~ '^(pg_|sql_)';
                    """)
        return(pd.DataFrame(cur.fetchall(), 
                            columns = ['table_name']))

    def show_columns(self,
                     table : str = 'topographicarea'):
        """
        The show_columns() method returns information about a table within the specified PostgreSQL database.

        table: The name of the table for which information is to be retrieved.
        """
        con = self.con
        cur = con.cursor()
        try:
            cur.execute(f"""
                        SELECT * 
                        FROM {table} 
                        LIMIT 0
                        """)
            colnames = [desc[0] for desc in cur.description]
        except:
            print(f'{table} does not exist!')
            return 
        return pd.DataFrame(colnames, 
                            columns = ['column_name'])

    def run_query(self,
                  query : str):
        """
        The run_query() method is used to execute a query on the specified PostgreSQL database.
        Result is returned as a list of tuples.

        query: The query to be executed.
        """
        con = self.con
        cur = con.cursor()
        cur.execute(query = query)
        return cur.fetchall()

    def from_postgres_to_geopandas(self,
                                   sql : str,
                                   geom_col : str,
                                   crs : str = 'epsg:2770'):
        con = self.con
        return gpd.read_postgis(sql = sql, 
                                con = con, 
                                crs = crs, 
                                geom_col = geom_col)

# MyBigQuery Class
class MyBigQuery:

    def __init__(self,
                 credentials_file : str = './credentials/New AutoMotive Index-487e031dc242.json'):
        """
        This is used to initialise an instance of the MyBigQuery class.

        credentials_files: The file containing the Google Cloud Service Account credentials.
        """
        self.project_id = 'rugged-baton-283921'
        self.bq_client = bigquery.Client(credentials = service_account.Credentials.from_service_account_file(credentials_file), 
                                         project = self.project_id)

    def from_bq_to_dataframe(self,
                             query : str):
        """
        The from_bq_to_dataframe() method is used to execute a query on the specified BigQuery project.
        Result is returned as a Pandas dataframe.

        sql_query: The query to be executed.
        """
        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()
        return df 
        
    def get_info_schema(self,
                        schema : str = 'mots_uk'):
        """
        The get_info_schema() method returns information about the tables within a dataset of the specified BigQuery project.

        schema: The name of the dataset for which table information is to be retrieved.
        """
        return self.from_bq_to_dataframe(query = f"""SELECT * FROM {self.project_id}.{schema}.INFORMATION_SCHEMA.TABLES;""")[['table_catalog', 'table_schema', 'table_name', 'creation_time']]
          
    def get_info_table(self,
                       schema : str = 'mots_uk',
                       table_name : str = 'vehicles_old'):
        """"
        The get_info_table() method returns information about a table within a dataset of the specified BigQuery project.

        schema: The name of the dataset where the table is located.
        table_name: The name of the table for which information is to be retrieved.
        """
        return self.from_bq_to_dataframe(query = f"""SELECT * FROM {self.project_id}.{schema}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name='{table_name}';""")[['table_schema', 'table_name', 'column_name', 'data_type']]

    def append_from_df(self,
                       table_name : str,
                       df : pd.DataFrame,
                       dataset_name : str = 'mots_uk',
                       job_config : Optional[dict] = None,
                       unique_fields : Optional[list] = None):
        """
        The append_from_df() method is used to append rows to a table within the specified dataset of the BigQuery project.

        table_name: The name of the target table.
        df: The dataframe containing the data to append.
        dataset_name: The name of the dataset where the target table is located.
        job_config: The configuration for the load job.
        unique_fields: A list of columns that should have unique values.

        Rows are appended in one of two ways:
        1). If the table does not exist, the method creates a new table and loads data into it.
        2). If the table exists, the method appends data to the existing table.
        If `unique_fields` is specified, rows with existing values in these fields will not be appended.
        """
        table_ref = self.bq_client.dataset(dataset_name).table(table_name)

        # check if the table exists
        table_exists = table_name in self.get_info_schema(schema = dataset_name).table_name.to_list()
        # if the table exists, append to the existing table
        if table_exists:
            print(f'Table {table_name} exists. Appending...')
            job_config = job_config or bigquery.LoadJobConfig()
            job_config.write_disposition = 'WRITE_APPEND'
            job_config.schema_update_options = ['ALLOW_FIELD_ADDITION']
            if unique_fields is not None:
                print('Ignoring duplicates...')
                columns = ','.join(unique_fields)
                # remove the temporary table if it already exists 
                self.bq_client.query(f"""DROP TABLE `{self.project_id}.{dataset_name}.{table_name}_to_append`""")
                # create a new temporary table storing the data to append
                self.append_from_df(table_name = f'{table_name}_to_append', 
                                    df = df, 
                                    dataset_name = dataset_name)
                join_conditions = ' AND '.join([f't1.{col} = t2.{col}' for col in columns.split(',')])
                where_conditions = ' IS NULL AND '.join([f't2.{col}' for col in columns.split(',')]) + ' IS NULL'
                query = f"""
                        WITH t2 AS(
                            SELECT DISTINCT {columns}
                            FROM `{self.project_id}.{dataset_name}.{table_name}`
                        )
                        SELECT t1.*
                        FROM `{self.project_id}.{dataset_name}.{table_name}_to_append` AS t1
                        LEFT JOIN t2 
                        ON {join_conditions}
                        WHERE {where_conditions};
                        """
                df = self.from_bq_to_dataframe(query)
                # remove the temporary table 
                self.bq_client.query(f"""DROP TABLE `{self.project_id}.{dataset_name}.{table_name}_to_append`""")
                print(f'{len(df)} rows to append.')
        # else, create a new table
        else:
            print(f'Table {table_name} does not exist. Creating a new one...')
            if job_config:
                job_config.write_disposition = 'WRITE_TRUNCATE'
            else:
                job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')

        # load the dataframe 
        try:
            bigquery_job = self.bq_client.load_table_from_dataframe(df, 
                                                                    table_ref, 
                                                                    job_config = job_config)
            bigquery_job.result()
            return
        except:
            print(f'ERROR: Cannot append rows to {table_name}.')

    def upsert(self, 
               table_name : str,
               df : pd.DataFrame,
               columns_to_match : list,
               schema : str = 'mots_uk',
               job_config : Optional[dict] = None
              ):
        """
        The upsert() method is used to update existing rows or insert new rows into a table within the specified dataset of the BigQuery project.
        If a matching record is found, its values are updated.
        If no matching record is found, a new record is inserted.
        
        table_name: The name of the target table.
        df: The dataframe containing the data to upsert.
        columns_to_match: A list of columns used to match records in the target table.
        schema: The name of the dataset where the target table is located.
        """
        # check the list of columns is valid
        df_target_info = self.get_info_table(table_name = table_name,
                                             schema = schema)
        
        # if the target table does not exist, create it
        if(df_target_info.shape[0] == 0):
            print(f'{table_name} does not exist. Proceeding with the creation of the table...\n\n')
            self.append_from_df(table_name = table_name, 
                                df = df, 
                                dataset_name = schema,
                                job_config=job_config
                               ) 
            return 
        if(all(elem in df_target_info.column_name.values for elem in columns_to_match)):
            print(f'{table_name} exists. Updating the table...\n\n')
            print(f'Creating temp table {table_name}_new_records...')
            # remove the temporary table if it already exists 
            self.bq_client.query(f"""DROP TABLE `{self.project_id}.{schema}.{table_name}_new_records`""")
            # create a new temporary table storing the data to upsert
            self.append_from_df(table_name = f'{table_name}_new_records', 
                                df = df, 
                                dataset_name = schema,
                                job_config=job_config
                               )
            
            on_statement = '(' + ' AND '.join([f'target.{col} = source.{col}' for col in columns_to_match]) + ')'
            columns = df.columns.to_list()
            set_columns = [col for col in columns if col not in columns_to_match]
            set_statement = ' AND '.join([f'target.{col} = source.{col}' for col in set_columns])
            insert_statement = '(' + ', '.join([f'{col}' for col in columns]) + ')'
            values_statement = '(' + ', '.join([f'source.{col}' for col in columns]) + ')'
            upsert_query = f"""
                            MERGE INTO `{self.project_id}.{schema}.{table_name}` AS target
                            USING `{self.project_id}.{schema}.{table_name}_new_records` AS source
                            ON {on_statement}
                            WHEN MATCHED THEN
                                UPDATE SET {set_statement}
                            WHEN NOT MATCHED THEN
                                INSERT {insert_statement}
                                VALUES {values_statement}
                            """
            # perform the upsertion
            self.bq_client.query(upsert_query)
            # remove the temporary table
            self.bq_client.query(f"""DROP TABLE `{self.project_id}.{schema}.{table_name}_new_records`""") 
            return
        else:
            print(f"'columns_to_match' must be a list of valid columns in {schema}.{table_name}.")
            return
import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pandas as pd
import re

SCOPES = ['https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/spreadsheets'
          ]


country2id = {'Brasil': '1GFs4e89aWkeiC1h3LeTX541cWjXbgCgw',
 'Peru': '1jaNuf8EieEtxFrXjGXDPsh8KgXvclcHH',
 'Ecuador': '15NYwaN5AKKf2Qrd7lXJq884B75gRuibJ',
 'Venezuela': '1sp_wR9gSlaZ4K71LT3fm6ASuB7GIbatz',
 'Paraguay': '1i1tdo5QaXGf4M9PiG-Sq-Hd6ejJ5UbEO',
 'Colombia': '10SmrjXCsaWIuTQhQ_uxMv9L_GXHSELXX',
 'Uruguay': '1Nj4m7sUvwPe1HBoTmO47GEhCK-nRv9CH',
 'Chile': '1-24kMxSGAV8HhDOX3nyKl7B2oCGyeDdE',
 'Bolivia': '1gqmZI2wOeB3ONWtEW2sd63B2dZHao3Y1',
 'Argentina': '1gMCRGuahf0IsCrGOf4rGxNsbwc2CLq8O'}

class driveConnection:

    def __init__(self,credentials_folder = '../credentials'):
        self.creds = None

        if os.path.exists(os.path.join(credentials_folder,'token.pickle')):
            with open(os.path.join(credentials_folder,'token.pickle'), 'rb') as token:
                self.creds = pickle.load(token)
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(os.path.join(credentials_folder,'credentials.json'), SCOPES)
                self.creds = flow.run_local_server(port=0)
            with open(os.path.join(credentials_folder,'token.pickle'), 'wb') as token: #create token.pickle
                pickle.dump(self.creds, token)

    def show_files(self,folders = False,files=True):
        service = build('drive', 'v3', credentials=self.creds)
        topFolderId = '1oc81vD7ZYYHNtPxnLALKfFX_-zIT3OAo'#id South America folder
        df_list = []
        
        def recursive_search(topFolderId):            
            items = []
            pageToken = ""
            while pageToken is not None:
                response = service.files().list(q=f"'{topFolderId}' in parents", pageSize=1000, pageToken=pageToken, fields="nextPageToken, files(id, name,mimeType,parents)").execute()
                items.extend(response.get('files', []))
                pageToken = response.get('nextPageToken')
            df = pd.DataFrame.from_records(items)
            df_list.append(df)
            if(df.shape[0]==0):
                return
            else:
                for next_id in df.id.to_list():
                    recursive_search(next_id)
    
        recursive_search(topFolderId=topFolderId)
        df = pd.concat(df_list)
        df.parents = df.parents.apply(lambda x: x[0])

        df = pd.merge(df[['id','name']].rename(columns={'id':'parents','name':'parent_name'}),df[['parents','name','id','mimeType']],left_on = 'parents',right_on='parents',how='right')
        df.rename(columns={'parents':'parent_id'},inplace=True)
        if(files and folders):
            return df
        if(files):
            return df[df.mimeType.str.contains('spreadsheet')]

        if(folders):
            return df[df.mimeType.str.contains('folder')]

        return 

    def pull_sheet_data(self,spreadsheet_id):
        service = build('sheets', 'v4', credentials=self.creds)
        res = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheetName = res.get("sheets", [])[0].get("properties").get("title")#read only first page
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,range=sheetName).execute()
        values = result.get('values', [])
        
        if not values:
            print('No data found.')
        else:
            rows = result
            data = rows.get('values')
            print("COMPLETE: Data copied")
            return data
    
    def get_df(self,spreadsheet_id):

        data = self.pull_sheet_data(spreadsheet_id)
        df = pd.DataFrame(data[1:]).iloc[:,:len(data[0])]
        df.columns = data[0]
        return df

    def create_spreadsheet(self,folder_id,spreadsheet_name):
        
        drive = build('drive', 'v3', credentials=self.creds)
        file_metadata = {
            'name': spreadsheet_name,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet',
        }
        res = drive.files().create(body=file_metadata).execute()
        return res['id']

    def push_csv(self,csv_path, folder_id,spreadsheet_name):

        spreadsheetId=self.create_spreadsheet(folder_id=folder_id,spreadsheet_name=spreadsheet_name)
        service = build('sheets', 'v4', credentials=self.creds)
        res = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
        sheetId = res.get("sheets", [])[0].get("properties").get("sheetId")

        df = pd.read_csv(csv_path).astype('str')

        body = {
            'requests': [{
                'updateCells': {
                    'start': {
                        'sheetId': sheetId,
                        'rowIndex': 0,  # adapt this if you need different positioning
                        'columnIndex': 0, # adapt this if you need different positioning
                    },
                    'rows': [],
                    'fields': 'userEnteredValue'
                }
            }]
        }
        for row in df.itertuples(index=True, name='Pandas'):
            new_row = {
                'values': []
            }
            for val in row:
                # if pd.isna(val):
                #     new_row['values'].append({
                #         'userEnteredValue': {
                #             'stringValue': 'NaN'
                #         }
                #     })
                # elif isinstance(val, int) or isinstance(val, float):
                #     new_row['values'].append({
                #         'userEnteredValue': {
                #             'numberValue': val
                #         }
                #     })

                new_row['values'].append({
                    'userEnteredValue': {
                        'stringValue': str(val).replace(".",',')
                    }
                })
            body['requests'][0]['updateCells']['rows'].append(new_row)
        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=body)
        response = request.execute()
        return response


    def push_csv2(self,csv_path, folder_id,spreadsheet_name):
        
        def replace_decimals(string):
            return re.sub(r"(-?\d+)\.(\d+)", '"\\1,\\2"', string)
        spreadsheetId=self.create_spreadsheet(folder_id=folder_id,spreadsheet_name=spreadsheet_name)
        service = build('sheets', 'v4', credentials=self.creds)
        res = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
        sheetId = res.get("sheets", [])[0].get("properties").get("sheetId")#

        with open(csv_path, 'r') as csv_file:
            csvContents = replace_decimals(csv_file.read())
        body = {
            'requests': [{
                'pasteData': {
                    "coordinate": {
                        "sheetId": sheetId,
                        "rowIndex": "0",  # adapt this if you need different positioning
                        "columnIndex": "0", # adapt this if you need different positioning
                    },
                    "data": csvContents,
                    "type": 'PASTE_NORMAL',
                    "delimiter": ',',
                }
            }]
        }
        print(sheetId)
        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=body)
        response = request.execute()
        return response

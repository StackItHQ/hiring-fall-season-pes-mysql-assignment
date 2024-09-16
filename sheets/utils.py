import os 
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import json


load_dotenv()

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

# Function to read data from Google Sheets
def read_all_from_sheet(sheet_name):
    RANGE_NAME = f'{sheet_name}!A1:Z'
    try:
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    except:
        print("Sheet does not exist.")
        return [], []
    values = result.get('values', [])
    column_names = values[0]
    return column_names,values


def clean_data_insert_delete(data,operation):
    json_data = data.replace(f'{operation}: ', '')

    parsed_data = json.loads(json_data)

    # Extract column names and values
    columns = list(parsed_data.keys())
    values = list(parsed_data.values())

    return columns, values

def clean_data_update(data):
    json_data = data.replace('UPDATE: ', '')

    parsed_data = json.loads(json_data)

    # Extract old and new values
    old_values = parsed_data['old']
    new_values = parsed_data['new']

    # Extract column names
    columns = list(old_values.keys())

    # Extract old and new values
    old_values = list(old_values.values())
    new_values = list(new_values.values())

    return columns, old_values, new_values
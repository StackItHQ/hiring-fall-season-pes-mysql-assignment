# sheets/google_sheets.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Authenticate and connect to Google Sheets
def connect_to_google_sheets(sheet_name, credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    
    sheet = client.open(sheet_name).sheet1  # Get the first sheet
    return sheet

# Read data from Google Sheets
def read_google_sheet(sheet):
    try:
        data = sheet.get_all_records()
        return data
    except Exception as e:
        print(f"Error reading Google Sheets: {e}")
        return None

# Write data to Google Sheets
def write_to_google_sheet(sheet, row, col, value):
    try:
        sheet.update_cell(row, col, value)
    except Exception as e:
        print(f"Error writing to Google Sheets: {e}")

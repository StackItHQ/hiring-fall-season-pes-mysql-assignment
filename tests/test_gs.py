# test_google_sheet_write.py

from sheets.google_sheets import connect_to_google_sheets, write_to_google_sheet
from config.settings import GOOGLE_SHEET_NAME, GOOGLE_SHEETS_CREDENTIALS_PATH

# Test writing data to Google Sheets
def test_google_sheet_write():
    # Step 1: Connect to Google Sheets
    sheet = connect_to_google_sheets(GOOGLE_SHEET_NAME, GOOGLE_SHEETS_CREDENTIALS_PATH)
    
    # Step 2: Write to Google Sheets (for example, to row 2, column 1)
    try:
        write_to_google_sheet(sheet, 2, 1, "Test Write from Python")
        print("Successfully wrote to Google Sheets.")
    except Exception as e:
        print(f"Failed to write to Google Sheets: {e}")

if __name__ == "__main__":
    test_google_sheet_write()

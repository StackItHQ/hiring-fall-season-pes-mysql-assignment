import json
from database import *
from sheets import *

def sync_sheets_to_db(sheet_name):
    column_names, data = read_all_from_sheet(sheet_name)
    if not data:
        print("Sheet does not exist in the document.")
        return
    table_name = sheet_name
    write_all_to_db(table_name,column_names,data)

def sync_db_to_sheets(table_name):
    column_names, data = read_from_db(table_name)
    if not data:
        print("Table does not exist in the database.")
        return
    sheet_name = table_name
    write_all_to_sheet(sheet_name,column_names,data)


def main():
    
    # Initialize the configuration dictionary
    config = {
        "google_sheets_priority": 0,
        "mysql_database_priority": 0
    }

    if os.path.exists('priority.json'):
        with open('priority.json', 'r') as config_file:
            config = json.load(config_file)
    
    else:
        while True:
            # Set the priority based on the user's choice
            choice = input("Do you prefer to start with Google Sheets or MySQL? (Enter 'google' or 'mysql'): ").strip().lower()
            if choice == 'google':
                config["google_sheets_priority"] = 1
                break
            elif choice == 'mysql':
                config["mysql_database_priority"] = 1
                break
            else:
                print("Invalid choice. Please enter 'google' or 'mysql'.")

        with open('priority.json', 'w') as config_file:
            json.dump(config, config_file, indent=4)
    

    if config["google_sheets_priority"] == 1:
        sheet_name = input("Enter the name of the Sheet you want to sync with the database: ")
        sync_sheets_to_db(sheet_name)
    
    elif config["mysql_database_priority"] == 1:
        table_name = input("Enter the name of the table you want to sync with Google Sheets: ")
        sync_db_to_sheets(table_name)

if __name__ == "__main__":
    main()
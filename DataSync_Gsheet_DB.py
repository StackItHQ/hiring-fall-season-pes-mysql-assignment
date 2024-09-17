import gspread
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pytz
import mysql.connector
import json

class RealTimeSync:

    def __init__(self):
        self.table_name = 'sheetData'

        # Path to your credentials JSON file obtained from google sheet
        self.CREDENTIALS_FILE = 'path to your credentials JSON file '
        
        # Define the scope and authenticate with the credentials file
        self.SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

        self.creds = Credentials.from_service_account_file(self.CREDENTIALS_FILE, scopes=self.SCOPES)

        self.client = client = gspread.authorize(self.creds)

        # Open the Google Sheet using the sheet ID from the URL
        self.spreadsheet_id = 'your_spreadsheet_id'

        self.sheet = client.open_by_key(self.spreadsheet_id).sheet1  # Open the first sheet

        # Build the Google Drive API service
        self.drive_service = build('drive', 'v3', credentials=self.creds)

        # Database connection 
        #Changes the config as mentioned
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password",
            database="your_databaseName"
        )

        self.cursor = self.db.cursor()


    # Function to generate the SQL for adding new columns
    def add_column_sql(self, column_name):
        return f"ALTER TABLE {self.table_name} ADD `{column_name}` VARCHAR(255);"

    # Function to generate the SQL for dropping columns
    def drop_column_sql(self, column_name):
        return f"ALTER TABLE {self.table_name} DROP COLUMN `{column_name}`;"

    # Function to generate the SQL for renaming columns
    def rename_column_sql(self, old_column, new_column):
        return f"ALTER TABLE {self.table_name} CHANGE `{old_column}` `{new_column}` VARCHAR(255);"

    def sanitize_rows(self, sheetRows, empty_string_indices):
        """
        Removes elements from each row based on indices where the column is an empty string.

        :param sheet_rows: List of rows, where each row is a list of column values
        :param empty_string_indices: List of indices where the column values are empty strings
        :return: Updated list of rows with empty string columns removed
        """

        sanitized_rows = []

        for row in sheetRows:
            # Remove elements at indices specified in empty_string_indices

            sanitized_row = [value for index, value in enumerate(row) if index not in empty_string_indices]
            if sanitized_row != []:
                sanitized_rows.append(sanitized_row)

        return sanitized_rows

    def generate_update_sql(self, unique_id, changes, id_column='id'):
        """
        Generates an SQL UPDATE query to update specific columns in the database based on changes.
        :return: The generated SQL UPDATE query as a string.
        """

        # Create a list of 'column = value' strings
        set_clause = ", ".join([f"`{column}` = %s" for column in changes])

        # Construct the final SQL query
        sql_query = f"UPDATE {self.table_name} SET {set_clause} WHERE `{id_column}` = %s"

        # Return both the query and the values (for parameterized query)
        return sql_query, list(changes.values()) + [unique_id]

    def updateValue_db(self, sheetRows, sheetColumns):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        db_data = self.cursor.fetchall()

        for i, row in enumerate(sheetRows):
            changes = {}
            unique_id = i + 1
            for j, ele in enumerate(row):
                if ele == db_data[i][j + 1]:
                    continue
                else:
                    changes[sheetColumns[j]] = ele
            if changes == {}:
                continue
            sql_query, params = self.generate_update_sql(unique_id, changes)

            self.cursor.execute(sql_query, params)
            self.db.commit()
    
    def fetch_timestamp_sheet(self):

        file_metadata = self.drive_service.files().get(fileId=self.spreadsheet_id, fields='modifiedTime').execute()

        # Print the last modified time
        last_modified_time = file_metadata['modifiedTime']
        print(f"The Google Sheet was last modified on: {last_modified_time}")

        # Convert the ISO time string to a datetime object (assumes UTC timezone because of the 'Z')
        utc_time = datetime.strptime(last_modified_time, '%Y-%m-%dT%H:%M:%S.%fZ')

        # Define UTC and IST timezones using pytz
        utc = pytz.timezone('UTC')
        ist = pytz.timezone('Asia/Kolkata')

        # Localize the UTC time and convert to IST
        utc_time = utc.localize(utc_time)
        ist_time = utc_time.astimezone(ist)

        # Output the IST time in a readable format
        return ist_time.strftime('%Y-%m-%d %H:%M:%S %Z')

    def fetch_max_time_stamp(self):
            # Find the max timestamp in the database
        query = f"""
        SELECT MAX(GREATEST(created_at, updated_at)) AS overall_latest_timestamp
        FROM {self.table_name};
        """

        # Execute the SQL query
        self.cursor.execute(query)

        # Fetch the result
        result = self.cursor.fetchone()

        # Extract the timestamp
        if result[0] is None:
            return None  # Handle cases where there are no timestamps

        # Assume the timestamp is already in IST, so no timezone conversion is needed
        timestamp = result[0]

        # Format the timestamp in the same way as fetch_timestamp_sheet
        # If it's a datetime object, we format it; if it's a string, we parse it first
        if isinstance(timestamp, datetime):
            return timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            # Convert string to datetime if necessary
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')

    def updateDb(self,sheetRows,sheetColumns):
        # print(sheetColumn)
            query = f"DESCRIBE {self.table_name}"
            self.cursor.execute(query)

            # Format: [('Col name1','datatype',...),(...),(...)]
            columnsDb = self.cursor.fetchall()
            
            db_column_names = [col[0] for col in columnsDb]

            # Filter out 'id', 'created_at', and 'updated_at' from db_column_names
            column_names = [col for col in db_column_names if col not in ['id', 'created_at', 'updated_at']]

            # Add columns to db
            add_columns = []

            remove_columns = []

            if (len(sheetColumns) > len(column_names)):
                start_index = len(column_names)
                add_columns = sheetColumns[start_index:]

            else:
                start_index = len(sheetColumns)
                remove_columns = column_names[start_index:]

            # Detect renamed columns
            renamed_columns = {}
            for sheet_col, db_col in zip(sheetColumns, column_names):
                if sheet_col != db_col:
                    renamed_columns[db_col] = sheet_col

            print(add_columns, remove_columns, renamed_columns)

            alter_queries = []

            for col in add_columns:
                alter_queries.append(self.add_column_sql(col))

            for col in remove_columns:
                alter_queries.append(self.drop_column_sql(col))

            # Iterate over the renamed_columns dictionary
            for old_col, new_col in renamed_columns.items():
                # Add the query to the list
                alter_queries.append(self.rename_column_sql(old_col, new_col))

            # Execute each SQL query to alter the table
            for query in alter_queries:
                print(f"Executing query: {query}")  # Print the query for debugging purposes
                self.cursor.execute(query)
                self.db.commit()

            query = f"SELECT COUNT(*) FROM {self.table_name};"

            # Execute the query
            self.cursor.execute(query)

            row_count = self.cursor.fetchone()[0]

            print(row_count)
            print(len(sheetRows))

            if row_count > len(sheetRows):

                self.updateValue_db(sheetRows, sheetColumns)
                query1 = f"SELECT id FROM (SELECT id FROM {self.table_name} ORDER BY id LIMIT {len(sheetRows)}) AS temp_table"
                self.cursor.execute(f"DELETE FROM {self.table_name} WHERE id NOT IN ({query1}) ")
                self.db.commit()

            else:

                self.updateValue_db(sheetRows, sheetColumns)

            self.cursor.close()

            print("data update in Db successfully")

    def updateSheet(self):
        # The range in the sheet where we want to update the values
        cell_range = 'H2:H10'  # This will update from row 2 to row 10 in column H

        # Define the new values to be inserted (as a list of lists)
        new_values = [[i] for i in range(4, 13)]  # Values from 2 to 10

        # Update the cells with the new values using named arguments
        self.sheet.update(range_name=cell_range, values=new_values)

        print("Column H updated successfully!")

    def updateSheetData(self):
        #Clean the sheet 
        self.sheet.clear()

        # Fetch the list of columns from the table
        self.cursor.execute("SHOW COLUMNS FROM sheetData")
        columns = self.cursor.fetchall()

        excluded_columns = {'id', 'created_at', 'updated_at'}

        # Extract column names excluding specified columns
        column_names = [col[0] for col in columns if col[0] not in excluded_columns]

        # Prepare the data to be updated in the first row
        data_to_insert = [column_names]

        # Fetch the list of columns from the table
        self.cursor.execute("SHOW COLUMNS FROM sheetData")
        columns = self.cursor.fetchall()

        # Update the Google Sheet starting from cell A1
        self.sheet.update('A1', data_to_insert)

        # Construct the SQL query to select all columns except the excluded ones
        columns_to_select = ', '.join(f"`{col}`" for col in column_names)
        query = f"SELECT {columns_to_select} FROM sheetData"

        # Execute the query
        self.cursor.execute(query)
        # Fetch all results
        results = self.cursor.fetchall()
        # Initialize a list to store column values
        column_values = [[] for _ in column_names]
        # Organize results into lists for each column
        for row in results:
            for i, value in enumerate(row):
                column_values[i].append(value)

        # Transpose the data: convert columns to rows
        transposed_values = list(map(list, zip(*column_values)))

        # Determine the starting row
        starting_row = 2

        # Update the Google Sheet starting from the specified row
        cell_range = f'A{starting_row}:{chr(64 + len(transposed_values[0]))}{starting_row + len(transposed_values) - 1}'
        self.sheet.update(cell_range, transposed_values)

    def main(self):

        # Fetch all data from the sheet
        data = self.sheet.get_all_values()

        # Extract column names and rows in Sheet
        sheetColumns = data[0]  # First row contains the column names
        sheetRows = data[1:]  # Subsequent rows contain the data

        check_table_query = f"""
            show tables like '{self.table_name}'
        """

        self.cursor.execute(check_table_query)

        # None when empty
        table_exists = self.cursor.fetchone()

        empty_string_indices = [index for index, column in enumerate(sheetColumns) if column == '']

        sheetColumns = [col.strip() for col in sheetColumns if col.strip()]

        sheetRows = self.sanitize_rows(sheetRows, empty_string_indices)

        #If Removed all the rows in sheet remove the table
        if len(sheetRows) == 0:
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")

        # Check if the table does not exists create a new table
        elif table_exists is None:
            # Drop the table if it already exists (optional)
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")

            # Dynamically create the table based on the column names
            # Assuming all columns are VARCHAR(255)
            column_definitions = "`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " + ", ".join(
                [f"`{col}` VARCHAR(255) NOT NULL DEFAULT '' " for col in
                 sheetColumns])  + ", `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " \
            "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"

            # Execute the create table query
            create_table_query = f"CREATE TABLE {self.table_name} ({column_definitions})"

            self.cursor.execute(create_table_query)

            print(f"Table '{self.table_name}' created successfully.")

            # Insert data into the newly created table
            for row in sheetRows:
                placeholders = ", ".join(["%s"] * len(sheetColumns))  # Creates '%s, %s, %s,...'
                column_names = ", ".join(
                    [f"`{col}`" for col in sheetColumns])  # Wrap column names in backticks for safety
                
                sql = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"
                # Execute the query with row values
                self.cursor.execute(sql, row)

            # Commit the transaction
            self.db.commit()

            # Close the cursor and connection
            self.cursor.close()
            self.db.close()

            print("Data inserted successfully!")

        else:
            max_timestamp=self.fetch_max_time_stamp()

            sheet_timeStamp=self.fetch_timestamp_sheet()

            print(sheet_timeStamp,max_timestamp)

            # Compare both timestamps 

            #if timestamp of sheet is latest update in db
            if  sheet_timeStamp > max_timestamp:
                self.updateDb(sheetRows,sheetColumns)

            #otherwise update in sheet
            else:
                self.updateSheetData()
                self.db.close()

if __name__ == '__main__':
    obj = RealTimeSync()
    obj.main()
    # obj.updateSheet()
    # print(obj.fetch_timestamp_sheet())

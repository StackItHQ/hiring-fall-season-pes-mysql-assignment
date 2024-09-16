from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import Error
import time
import threading
from datetime import datetime

app = Flask(__name__)

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def initialize_gsheet():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        sheet_id = '18tLIrvqMoOEsqwai588PXjd8D8R2UAoqFAj8dHy4Lv8'  
        sheet = client.open_by_key(sheet_id).sheet1
        print("Successfully connected to Google Sheet.")
        return sheet
    except Exception as e:
        print(f"Error connecting to Google Sheet: {e}")
        return None

sheet = initialize_gsheet()

# MySQL database connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='VeenaUG@28',
            database='mydatabase'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def read_users():
    connection = get_db_connection()
    if not connection:
        return []
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def read_users_gsheet():
    if not sheet:
        return []
    return sheet.get_all_records()

def insert_user(id, name, age, email, updated_at):
    connection = get_db_connection()
    if not connection:
        return
    cursor = connection.cursor()
    query = "INSERT INTO user_data (id, name, age, email, updated_at) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(query, (id, name, age, email, updated_at))
    connection.commit()
    cursor.close()
    connection.close()

def update_user(id, name, age, email, updated_at):
    connection = get_db_connection()
    if not connection:
        return
    cursor = connection.cursor()
    query = "UPDATE user_data SET name = %s, age = %s, email = %s, updated_at = %s WHERE id = %s"
    cursor.execute(query, (name, age, email, updated_at, id))
    connection.commit()
    cursor.close()
    connection.close()

def delete_user(id):
    connection = get_db_connection()
    if not connection:
        return
    cursor = connection.cursor()
    query = "DELETE FROM user_data WHERE id = %s"
    cursor.execute(query, (id,))
    connection.commit()
    cursor.close()
    connection.close()

def insert_user_gsheet(id, name, age, email, updated_at):
    if not sheet:
        return
    sheet.append_row([id, name, age, email, updated_at])

def update_user_gsheet(id, name, age, email, updated_at):
    if not sheet:
        return
    sheet_data = sheet.get_all_records()
    
    for index, row in enumerate(sheet_data):
        if row['id'] == id:
            sheet.update_cell(index + 2, 2, name)
            sheet.update_cell(index + 2, 3, age)
            sheet.update_cell(index + 2, 4, email)
            sheet.update_cell(index + 2, 5, updated_at)
            break

def delete_user_gsheet(id):
    if not sheet:
        return
    sheet_data = sheet.get_all_records()
    
    for index, row in enumerate(sheet_data):
        if row['id'] == id:
            sheet.delete_rows(index + 2)  # Adjusting for header row
            break

def sync_data():
    while True:
        try:
            if not sheet:
                print("Google Sheet not initialized.")
                break

         
            mysql_data = read_users()
            gsheets_data = read_users_gsheet()

          
            mysql_ids = {user['id'] for user in mysql_data}
            gsheets_ids = {row['id'] for row in gsheets_data}

           
            for row in gsheets_data:
                if 'id' in row and 'updated_at' in row:
                    updated_at_str = row['updated_at'].isoformat() if isinstance(row['updated_at'], datetime) else row['updated_at']
                    if row['id'] in mysql_ids:
                        update_user(row['id'], row['name'], row['age'], row['email'], updated_at_str)
                    else:
                        insert_user(row['id'], row['name'], row['age'], row['email'], updated_at_str)

            # Sync MySQL -> Google Sheets (Insert/Update)
            for user in mysql_data:
                updated_at_str = user['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(user['updated_at'], datetime) else user['updated_at']
                if user['id'] in gsheets_ids:
                    update_user_gsheet(user['id'], user['name'], user['age'], user['email'], updated_at_str)
                else:
                    insert_user_gsheet(user['id'], user['name'], user['age'], user['email'], updated_at_str)

            
            for user_id in mysql_ids - gsheets_ids:
                delete_user(user_id)
                delete_user_gsheet(user_id)

            print("Sync complete.")
        except Exception as e:
            print(f"Error during sync: {e}")
        time.sleep(60)  
        
def start_background_sync():
    sync_thread = threading.Thread(target=sync_data)
    sync_thread.daemon = True
    sync_thread.start()

@app.route('/')
def index():
    return "Flask app with background sync running."

if __name__ == '__main__':
    start_background_sync()
    app.run(port=3000, debug=True)

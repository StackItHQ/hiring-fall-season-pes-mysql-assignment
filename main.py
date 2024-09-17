import sys
import argparse
import gspread
from google.oauth2.service_account import Credentials
from MySQL_Connection import connect_to_mysql
from functions import *
import time

conn=connect_to_mysql()
sql = conn.cursor()


parser = argparse.ArgumentParser()
parser.add_argument("--url", required=True, help="google sheet url")
parser.add_argument("--cred", required=True, help="credentials json file")
args = parser.parse_args()
url = args.url
cred = args.cred
scope = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(cred, scopes=scope)
client = gspread.authorize(creds)


try:
    sheet = client.open_by_url(url)
except:
    print("Run python3 main.py --url <google sheet url>")
    sys.exit()


try:
    database = sheet.title
    query = f"CREATE DATABASE IF NOT EXISTS {database};"
    sql.execute(query)
    query = f"USE {database};"
    sql.execute(query)
    conn.commit()
except:
    print('creation of database failed...')


try:
    table = sheet.get_worksheet(0)
    table_name = table.title
except:
    print('getting table from google sheets failed...')


while True:


    query = f"SELECT * FROM per_hash;"
    sql.execute(query)
    data = sql.fetchall()
    per_hash = data[0][0]
    # print(per_hash)
    new_hash = per_hash


    # google sheet to sql
    data = table.get_all_values()
    hash = hash_list(data)
    # print("google", data, "---->", hash)

    if hash != per_hash:
        new_hash = hash
        update_sql(data, table_name, sql, conn)
        
        
    # sql to google sheet
    query = f"USE {database};"
    sql.execute(query)
    conn.commit()
    rows = get_sql_rows(sql, table_name)
    headers = get_sql_headers(sql, table_name)
    data = [headers] + rows
    hash = hash_list(data)
    # print("sql", data, "---->", hash)

    if hash != per_hash:
        new_hash = hash
        update_google(table, data)
            

    if new_hash != per_hash:
        query = f"UPDATE per_hash SET hash = '{new_hash}' WHERE hash = '{per_hash}';"
        sql.execute(query)
        conn.commit()


    time.sleep(5)

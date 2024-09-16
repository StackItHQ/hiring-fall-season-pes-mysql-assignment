import pandas as pd
from sqlalchemy import create_engine
import pygsheets

# MySQL connection
# Ensure you're using the correct driver, 'mysql+pymysql' is a common choice
my_conn = create_engine("mysql+pymysql://root:pes2ug21cs930@127.0.0.1/Project")

# Query MySQL
query = "SELECT * FROM details"
df = pd.read_sql(query, my_conn)

# Google Sheets connection
path = 'credentials.json'
gc = pygsheets.authorize(service_account_file=path)
sh = gc.open('GoogleSheets')
wk1 = sh[0]
wk1.clear()  # Clear existing data
wk1.set_dataframe(df, (1, 1), copy_index=True, extend=True)
print("Data successfully written to Google Sheets.")

# Fetch data from Google Sheets
df_gsheet = wk1.get_as_df(index_column=0)  # Ensure index_column is set properly
print("Data fetched from Google Sheets:")
print(df_gsheet)

# Fetch existing IDs from the 'details' table to avoid duplicates
existing_df = pd.read_sql("SELECT id FROM details", my_conn)

# Remove rows from Google Sheets dataframe that have duplicate IDs in MySQL
df_gsheet = df_gsheet[~df_gsheet['id'].isin(existing_df['id'])]
print("Data after removing duplicates:")
print(df_gsheet)

# Insert the remaining data into MySQL
try:
    with my_conn.connect() as conn:
        conn = conn.execution_options(autocommit=False)  # Disable autocommit
        trans = conn.begin()
        
        df_gsheet.to_sql(name='details', con=conn, if_exists='append', index=False)
        trans.commit()  # Commit changes
        print("Data successfully written to the 'details' table in MySQL.")
        
except Exception as e:
    print(f"An error occurred: {e}")

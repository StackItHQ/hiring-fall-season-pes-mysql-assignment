import requests
import pymysql

# Step 1: Connect to MySQL database using pymysql
def connect_to_mysql(host, database, user, password):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=database
        )
        if connection.open:
            print("Connected to MySQL database")
            return connection
    except Exception as e:
        print(f"Error while connecting to MySQL: {e}")
    return None

# Step 2: Insert/Update data into MySQL
def insert_data_into_mysql(connection, table_name, data):
    cursor = connection.cursor()

    # Access the 'values' from the Google Sheets data
    values = data['values']

    if not values:
        print("No data found in the Google Sheets response.")
        return

    # The first row contains the column names
    columns = values[0]
    rows = values[1:]

    # SQL query to delete all existing data in the table
    delete_query = f"DELETE FROM {table_name}"

    # Build SQL insert query dynamically
    placeholders = ', '.join(['%s'] * len(columns))
    columns_string = ', '.join(columns)
    sql_query = f"INSERT INTO {table_name} ({columns_string}) VALUES ({placeholders})"

    try:
        # Step 1: Delete all data from the table
        cursor.execute(delete_query)
        print(f"Deleted all rows from {table_name}")

        # Step 2: Insert the new data
        for row in rows:
            cursor.execute(sql_query, tuple(row))

        # Commit the transaction
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows into {table_name}")
    except pymysql.MySQLError as e:
        print(f"Error while inserting data: {e}")
        connection.rollback()


# Step 3: Sync function to tie everything together
def sync_google_sheet_to_mysql(data, mysql_config, table_name):
    # Connect to MySQL using pymysql
    connection = connect_to_mysql(**mysql_config)
    if connection:
        # Insert data into MySQL
        insert_data_into_mysql(connection, table_name, data)
        # Close the connection
        connection.close()
    else:
        print("Failed to connect to MySQL")

    # print("inerting data  - ",data)

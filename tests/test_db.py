import psycopg2
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

def connect_to_db():
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def create_table(connection):
    try:
        cursor = connection.cursor()
        # Create a new table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INT NOT NULL
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully.")
        
    except Exception as error:
        print("Error creating table:", error)

    finally:
        cursor.close()

def reset_table(connection):
    try:
        cursor = connection.cursor()
        # Clear existing data from the table
        cursor.execute("DELETE FROM test_table;")

        # Insert new data with specified IDs
        new_data = [
            (1, "Alice", 30),
            (2, "Bob", 25),
            (3, "Charlie", 35)
        ]

        insert_query = "INSERT INTO test_table (id, name, age) VALUES (%s, %s, %s);"
        cursor.executemany(insert_query, new_data)
        connection.commit()
        print("Table reset and populated with new values successfully.")
        
    except Exception as error:
        print("Error resetting table:", error)

    finally:
        cursor.close()

def read_data(connection):
    try:
        cursor = connection.cursor()
        # Read data from the table
        select_query = "SELECT * FROM test_table;"
        cursor.execute(select_query)
        records = cursor.fetchall()
        
        print("Data from test_table:")
        for row in records:
            print(row)
        
    except Exception as error:
        print("Error reading data:", error)

    finally:
        cursor.close()

if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        create_table(conn)
        reset_table(conn)  # Clear old values and add new ones
        read_data(conn)
        conn.close()
        print("PostgreSQL connection closed.")

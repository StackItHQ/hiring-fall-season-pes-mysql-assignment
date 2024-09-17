import mysql.connector #type:ignore

def connect_to_mysql():

    db_config = {
        "host": "localhost",
        "user": "superjoin",
        "password": "superjoin"
    }

    conn = mysql.connector.connect(**db_config)
    return conn
        
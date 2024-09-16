from db_conn import create_connection, close_connection

if __name__ == "__main__":
    conn, cursor = create_connection()
    if conn:
        # Execute a test query
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"PostgreSQL Database version: {db_version}")
        
        # Close the connection
        close_connection(conn, cursor)

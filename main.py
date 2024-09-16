from db.db_conn import create_connection, close_connection, create_employee, read_all_employees, update_employee, delete_employee
from sheets.google_sheets import connect_to_google_sheets, read_google_sheet, write_to_google_sheet

def sync_google_sheets_to_postgres(sheet):
    conn, cursor = create_connection()
    if conn and cursor:
        # Read data from Google Sheets
        data = read_google_sheet(sheet)
        for row in data:
            # Assuming 'name', 'role', and 'salary' are the columns in Google Sheets
            create_employee(cursor, conn, row['name'], row['role'], row['salary'])
        close_connection(conn, cursor)

def sync_postgres_to_google_sheets(sheet):
    conn, cursor = create_connection()
    if conn and cursor:
        # Read data from PostgreSQL
        employees = read_all_employees(cursor)
        # Write data to Google Sheets
        write_to_google_sheet(sheet, employees)
        close_connection(conn, cursor)

if __name__ == "__main__":
    # Connect to Google Sheets
    sheet = connect_to_google_sheets("superjoin", "/home/mayank/Documents/aupwejoin/credentials.json")
    
    # Sync from Google Sheets to PostgreSQL
    sync_google_sheets_to_postgres(sheet)

    # Sync from PostgreSQL to Google Sheets
    sync_postgres_to_google_sheets(sheet)

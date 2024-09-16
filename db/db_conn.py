import psycopg2

# Connect to the PostgreSQL Database
def create_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="sheet_sync_db",
            user="sheet_sync_user",
            password="mayank"
        )
        cursor = conn.cursor()
        return conn, cursor
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to PostgreSQL: {error}")
        return None, None

# Close the PostgreSQL connection
def close_connection(conn, cursor):
    if conn:
        cursor.close()
        conn.close()

# Create Operation
def create_employee(cursor, conn, name, role, salary):
    try:
        query = "INSERT INTO employees (name, role, salary) VALUES (%s, %s, %s)"
        cursor.execute(query, (name, role, salary))
        conn.commit()
        print(f"Employee '{name}' created successfully.")
    except Exception as e:
        print(f"Error creating employee: {e}")
        conn.rollback()

# Read Operation
def read_all_employees(cursor):
    try:
        query = "SELECT * FROM employees"
        cursor.execute(query)
        results = cursor.fetchall()
        print("Employees:")
        for row in results:
            print(row)
        return results
    except Exception as e:
        print(f"Error reading employees: {e}")
        return []

# Update Operation
def update_employee(cursor, conn, employee_id, name=None, role=None, salary=None):
    try:
        # Dynamically build the query based on provided arguments
        query = "UPDATE employees SET"
        fields = []
        values = []
        
        if name:
            fields.append(" name = %s")
            values.append(name)
        if role:
            fields.append(" role = %s")
            values.append(role)
        if salary:
            fields.append(" salary = %s")
            values.append(salary)
        
        query += ",".join(fields) + " WHERE id = %s"
        values.append(employee_id)
        
        cursor.execute(query, tuple(values))
        conn.commit()
        
        print(f"Employee with ID {employee_id} updated successfully.")
    except Exception as e:
        print(f"Error updating employee: {e}")
        conn.rollback()

# Delete Operation
def delete_employee(cursor, conn, employee_id):
    try:
        query = "DELETE FROM employees WHERE id = %s"
        cursor.execute(query, (employee_id,))
        conn.commit()
        print(f"Employee with ID {employee_id} deleted successfully.")
    except Exception as e:
        print(f"Error deleting employee: {e}")
        conn.rollback()

# Example Usage of CRUD functions
if __name__ == "__main__":
    conn, cursor = create_connection()

    if conn and cursor:
        # Create
        create_employee(cursor, conn, "John Doe", "Manager", 75000)

        # Read
        employees = read_all_employees(cursor)

        # Update
        if employees:
            employee_id = employees[0][0]  # Assuming the first employee's ID
            update_employee(cursor, conn, employee_id, salary=80000)

        # Delete
        if employees:
            employee_id = employees[0][0]
            delete_employee(cursor, conn, employee_id)

        # Close connection
        close_connection(conn, cursor)

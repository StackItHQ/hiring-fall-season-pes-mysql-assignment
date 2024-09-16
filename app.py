from flask import Flask, request, jsonify
import psycopg2
import select
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

app = Flask(__name__)

def connect_to_db():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

@app.route('/update_google_sheets', methods=['POST'])
def update_google_sheets():
    data = request.get_json()
    # Here, you would update your PostgreSQL database using psycopg2
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO test_table (id, name, age) VALUES (%s, %s, %s);", 
                   (data['id'], data['name'], data['age']))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"status": "success", "message": "Data updated successfully."}), 200

def listen_for_changes():
    conn = connect_to_db()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("LISTEN data_change;")
    print("Waiting for notifications on channel 'data_change'...")

    while True:
        select.select([conn], [], [])
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            print(f"Received notification: {notify.payload}")
            # Here, you can handle the received notification (e.g., fetch updated data and send it to Google Sheets)

if __name__ == '__main__':
    app.run(debug=True)
    listen_for_changes()  # Start listening for PostgreSQL notifications

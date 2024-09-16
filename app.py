from flask import Flask, request, jsonify
from connecter import sync_google_sheet_to_mysql

app = Flask(__name__)

# Route to handle GET requests at the root URL
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Welcome to the Flask app! It's running."}), 200

# Route to handle POST requests from Google Sheets webhook
@app.route('/webhook', methods=['POST'])
def google_sheet_webhook():
    mysql_config = {
        "host": "localhost",
        "database": "superjoin",
        "user": "root",
        "password": "2003"
    }
    table_name = "student"
    
    data = request.json  # Get the JSON data sent from Google Sheets
    if not data or 'values' not in data:
        return jsonify({"status": "error", "message": "Invalid data"}), 400
    
    # Call the sync function to insert data into MySQL
    try:
        sync_google_sheet_to_mysql(data, mysql_config, table_name)
        return jsonify({"status": "success", "message": "Data received and inserted"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000)


# from flask import Flask, request, jsonify
# import logging
# import threading
# import mysql.connector
# import psycopg2
# from twoWayKafka import KafkaHandler
# from dbCode.mysqlScript import sync_sheet_from_json
# from updateSheet import logUpdate

# logging.basicConfig(level=logging.INFO)


# # --- Database Configurations ---

# # MySQL Database Configuration
# mysql_db = mysql.connector.connect(
#     host="localhost",        # MySQL server host
#     user="root",             # MySQL username
#     password="Aavish@02",    # MySQL password
#     database="superjoin"  # Database name
# )




# # --- Kafka Consumer Thread Function ---

# def run_consumer(kafka_handler):
#     def process_message(msg):
#         # Process the message
#         sync_sheet_from_json(mysql_db, msg)
        
#         # Prepare response
#         correlation_id = msg.get('correlation_id')
#         if correlation_id:
#             response = {
#                 'status': 'success',
#                 'correlation_id': correlation_id,
#                 'result': 'Data synchronized'
#             }
#             # Send response back to the response_topic
#             kafka_handler.producer.send(kafka_handler.response_topic, response)
#             kafka_handler.producer.flush()
    
#     kafka_handler.consume_messages(process_message)

    

# # --- Flask Application ---

# app = Flask(__name__)

# # Create a KafkaHandler instance with response_topic for two-way communication
# kafka_handler = KafkaHandler(
#     bootstrap_servers='localhost:9092',
#     topic='my_topic',
#     group_id='my_group',
#     response_topic='response_topic'  # Replace with your actual response topic
# )

# # Start Kafka consumer in a background thread
# consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
# consumer_thread.start()

# # db_trigger = threading.Thread(target=logUpdate, daemon=True)
# # db_trigger.start()


# # Flask route to publish message to Kafka and wait for a response
# @app.route('/kafka-publish-endpoint', methods=['POST'])
# def kafka_publish():
#     """
#     Endpoint to publish a message to Kafka and wait for a response.
#     Expects a JSON payload in the request.
#     """
#     data = request.json
#     if not data:
#         return jsonify({"status": "error", "message": "Invalid payload"}), 400

#     # Send a request and wait for a response
#     response = kafka_handler.send_request(data, timeout=10)
#     return jsonify(response)

# # Start Flask server
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)
from flask import Flask, request, jsonify
import logging
import threading
import mysql.connector
import json
import time
from twoWayKafka import KafkaHandler
from dbCode.mysqlScript import sync_sheet_from_json
# Uncomment the following line if you need to use logUpdate
# from updateSheet import logUpdate

logging.basicConfig(level=logging.INFO)

# --- Database Configurations ---

# MySQL Database Configuration
db_config = {
    'host': 'localhost',        # MySQL server host
    'user': 'root',             # MySQL username
    'password': 'Aavish@02',    # MySQL password
    'database': 'superjoin'     # Database name
}

# --- Kafka Configuration ---

kafka_config = {
    'bootstrap_servers': 'localhost:9092',
    'topic': 'my_topic',
    'group_id': 'my_group',
    'response_topic': 'response_topic'  # Replace with your actual response topic
}

# --- Flask Application ---

app = Flask(__name__)

# Create a KafkaHandler instance with response_topic for two-way communication
kafka_handler = KafkaHandler(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    topic=kafka_config['topic'],
    group_id=kafka_config['group_id'],
    response_topic=kafka_config['response_topic']
)

# --- Function to Read and Write Last Processed ID ---

def read_last_id():
    try:
        with open('last_id.txt', 'r') as f:
            return int(f.read())
    except Exception:
        return 0

def write_last_id(last_id):
    with open('last_id.txt', 'w') as f:
        f.write(str(last_id))

# --- Cells Monitoring Function ---

def monitor_cells(kafka_handler, db_config):
    last_id = read_last_id()  # Read from file or initialize to 0
    while True:
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor(dictionary=True)
            query = """
                SELECT * FROM cells
                WHERE changed_by = 'sheet_sync_user@localhost'
                  AND operation = 'insert'
                  AND id > %s
                ORDER BY id ASC
            """
            cursor.execute(query, (last_id,))
            changes = cursor.fetchall()
            cursor.close()
            connection.close()
            if changes:
                for change in changes:
                    # Log the addition
                    logging.info(f"Cell added by 'sheet_sync_user': {change}")

                    message = {
                        'id': change['id'],
                        'sheet_id': change['sheet_id'],
                        'row_number': change['row_number'],
                        'column_number': change['column_number'],
                        'value': change['value'],
                        'operation': change['operation'],
                        'changed_by': change['changed_by'],
                        'changed_at': change['changed_at'].strftime('%Y-%m-%d %H:%M:%S'),
                        'is_current': change['is_current']
                    }
                    # Publish message to Kafka using KafkaHandler
                    kafka_handler.publish_message(message)
                    logging.info(f"Sent message: {message}")

                last_id = changes[-1]['id']
                write_last_id(last_id)  # Save the last processed ID
            else:
                time.sleep(1)
        except Exception as e:
            logging.error(f"Error in monitor_cells: {e}")
            time.sleep(1)

# Start the monitor_cells function in a background thread
monitor_thread = threading.Thread(target=monitor_cells, args=(kafka_handler, db_config), daemon=True)
monitor_thread.start()

# --- Kafka Consumer Thread Function ---

def run_consumer(kafka_handler):
    def process_message(msg):
        # Process the message
        sync_sheet_from_json(mysql.connector.connect(**db_config), msg)
        
        # Prepare response
        correlation_id = msg.get('correlation_id')
        if correlation_id:
            response = {
                'status': 'success',
                'correlation_id': correlation_id,
                'result': 'Data synchronized'
            }
            # Send response back to the response_topic
            kafka_handler.producer.send(kafka_handler.response_topic, response)
            kafka_handler.producer.flush()
    
    kafka_handler.consume_messages(process_message)

# Start Kafka consumer in a background thread
consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
consumer_thread.start()

# Uncomment the following lines if you need to use logUpdate
# db_trigger = threading.Thread(target=logUpdate, daemon=True)
# db_trigger.start()

# --- Flask Route to Publish Message to Kafka and Wait for a Response ---

@app.route('/kafka-publish-endpoint', methods=['POST'])
def kafka_publish():
    """
    Endpoint to publish a message to Kafka and wait for a response.
    Expects a JSON payload in the request.
    """
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "Invalid payload"}), 400

    # Send a request and wait for a response
    response = kafka_handler.send_request(data, timeout=10)
    return jsonify(response)

# Start Flask server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

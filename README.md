# Real-Time Data Synchronization between Google Sheets and PostgreSQL

## Overview

This project provides a real-time data synchronization solution between Google Sheets and a PostgreSQL database. It allows you to seamlessly update and reflect changes made in either Google Sheets or PostgreSQL, ensuring that both data sources remain in sync.

## Features

- [x] My code's working just fine! 🥳
- [x] I have recorded a video showing it working and embedded it in the README ▶️
- [x] I have tested all the normal working cases 😎
- [x] I have even solved some edge cases (brownie points) 💪
- [x] I added my very planned-out approach to the problem at the end of this README 📜

## Technologies Used

- Google Apps Script for interacting with Google Sheets.
- Flask as a lightweight web server for handling API requests.
- Psycopg2 for PostgreSQL database interaction.

## Setup Instructions

### Prerequisites

- Python 3.x
- PostgreSQL
- Google Sheets API enabled and credentials set up
- Flask
- Psycopg2

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/SlackitHQ/pes-Memomer.git
   cd pes-Memomer

   
## Video File link
https://github.com/StackItHQ/pes-Memomer/tree/mayank/video_recording
https://drive.google.com/file/d/1FN8Da-DctZ8_PjRxjlejWGU6mEouBBLx/view?usp=sharing

## Planned Approach

The primary objective of this project is to achieve real-time data synchronization between Google Sheets and a PostgreSQL database. The approach is structured as follows:

1. **Define Data Structure**:
   - Create a table in PostgreSQL (`test_table`) with three columns: `id`, `name`, and `age`. The table will serve as the main data structure for both Google Sheets and PostgreSQL.
   - Initialize the PostgreSQL table with predefined data (e.g., Alice, Bob, Charlie) and ensure that the IDs are sequential (1 to 3).

2. **Set Up Google Sheets**:
   - Create a Google Sheet that mirrors the PostgreSQL table structure, ensuring that it has the same three columns.
   - Use Google Apps Script to write an `onEdit` function that will trigger whenever changes are made in the Google Sheet.

3. **Create a Flask API**:
   - Set up a Flask server to handle incoming requests from Google Sheets and perform updates to the PostgreSQL database.
   - Implement an API endpoint (`/update_google_sheets`) that accepts POST requests containing the updated data from Google Sheets.

4. **Establish Database Connection**:
   - Use `psycopg2` to connect the Flask server with the PostgreSQL database, enabling the server to perform CRUD operations based on incoming requests.

5. **Implement Two-Way Synchronization**:
   - Enable real-time updates from Google Sheets to PostgreSQL:
     - When data is entered or modified in the Google Sheet, the `onEdit` function will send the updated data to the Flask API, which will then update the PostgreSQL table.
   - Enable real-time updates from PostgreSQL to Google Sheets:
     - Set up a mechanism (e.g., a scheduled job) to periodically check for changes in the PostgreSQL table and update the corresponding Google Sheet.

6. **Testing and Validation**:
   - Test the integration thoroughly by adding, updating, and deleting records in both Google Sheets and PostgreSQL.
   - Ensure that changes are reflected accurately and in real-time in both systems.

7. **Deployment**:
   - Use ngrok to expose the local Flask server to the internet, allowing Google Sheets to communicate with it.
   - Document the deployment process and provide instructions for setting up the server in different environments.

8. **Documentation**:
   - Maintain clear and comprehensive documentation for the project, including setup instructions, API endpoints, and contribution guidelines.


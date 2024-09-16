[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/AHFn7Vbn)
# Superjoin Hiring Assignment

### Welcome to Superjoin's hiring assignment! 🚀

### Objective
Build a solution that enables real-time synchronization of data between a Google Sheet and a specified database (e.g., MySQL, PostgreSQL). The solution should detect changes in the Google Sheet and update the database accordingly, and vice versa.

### Problem Statement
Many businesses use Google Sheets for collaborative data management and databases for more robust and scalable data storage. However, keeping the data synchronised between Google Sheets and databases is often a manual and error-prone process. Your task is to develop a solution that automates this synchronisation, ensuring that changes in one are reflected in the other in real-time.

### Requirements:
1. Real-time Synchronisation
  - Implement a system that detects changes in Google Sheets and updates the database accordingly.
   - Similarly, detect changes in the database and update the Google Sheet.
  2.	CRUD Operations
   - Ensure the system supports Create, Read, Update, and Delete operations for both Google Sheets and the database.
   - Maintain data consistency across both platforms.
   
### Optional Challenges (This is not mandatory):
1. Conflict Handling
- Develop a strategy to handle conflicts that may arise when changes are made simultaneously in both Google Sheets and the database.
- Provide options for conflict resolution (e.g., last write wins, user-defined rules).
    
2. Scalability: 	
- Ensure the solution can handle large datasets and high-frequency updates without performance degradation.
- Optimize for scalability and efficiency.

## Submission ⏰
The timeline for this submission is: **Next 2 days**

Some things you might want to take care of:
- Make use of git and commit your steps!
- Use good coding practices.
- Write beautiful and readable code. Well-written code is nothing less than a work of art.
- Use semantic variable naming.
- Your code should be organized well in files and folders which is easy to figure out.
- If there is something happening in your code that is not very intuitive, add some comments.
- Add to this README at the bottom explaining your approach (brownie points 😋)
- Use ChatGPT4o/o1/Github Co-pilot, anything that accelerates how you work 💪🏽. 

Make sure you finish the assignment a little earlier than this so you have time to make any final changes.

Once you're done, make sure you **record a video** showing your project working. The video should **NOT** be longer than 120 seconds. While you record the video, tell us about your biggest blocker, and how you overcame it! Don't be shy, talk us through, we'd love that.

We have a checklist at the bottom of this README file, which you should update as your progress with your assignment. It will help us evaluate your project.

- [ ] My code's working just fine! 🥳
- [ ] I have recorded a video showing it working and embedded it in the README ▶️
- [ ] I have tested all the normal working cases 😎
- [ ] I have even solved some edge cases (brownie points) 💪
- [ ] I added my very planned-out approach to the problem at the end of this README 📜

## Got Questions❓
Feel free to check the discussions tab, you might get some help there. Check out that tab before reaching out to us. Also, did you know, the internet is a great place to explore? 😛

We're available at techhiring@superjoin.ai for all queries. 

All the best ✨.

## Developer's Section
![Description of the image](assets/logo.png)
- **My code's working just fine!** 🥳  
  - The synchronization between Google Sheets and PostgreSQL is fully operational. Changes made in Google Sheets are reflected in PostgreSQL and vice versa, with real-time updates handled by PostgreSQL notifications.

- **I have recorded a video showing it working and embedded it in the README** ▶️  
  - [View Video Demonstration](assets/super-join-clip.mp4)
  - [Drive Link](https://drive.google.com/file/d/1xo6nvCiMRB7gVmgC0WKJYmep86G4m0ez/view?usp=drive_link)
  - video is 60 seconds more than required, tried to cover complete working demo of assignment, sorry for that!

- **I have tested all the normal working cases** 😎  
  - Normal cases tested include:
    - Adding, updating, and deleting rows in Google Sheets and verifying that changes are accurately reflected in PostgreSQL.
    - Performing the same operations in PostgreSQL and ensuring that Google Sheets is updated accordingly.

- **I have even solved some edge cases (brownie points)** 💪  
  - Edge cases handled include:
    - Handling cases where rows in Google Sheets are empty or have incomplete data.
    - Managing simultaneous updates where changes are made to both Google Sheets and PostgreSQL at the same time.
    - Ensuring data consistency and integrity even when network issues or other errors occur.

- **I added my very planned-out approach to the problem at the end of this README** 📜  

### Approach Used

#### 1. **Bidirectional Synchronization**

To ensure that Google Sheets and PostgreSQL remain synchronized, I implemented bidirectional synchronization using two FastAPI endpoints:

- **`/sync_postgres`:** Synchronizes data from Google Sheets to PostgreSQL.
  - Fetches data from Google Sheets.
  - Inserts or updates data in PostgreSQL using the `ON CONFLICT` clause to handle conflicts based on the primary key (`lead_id`).
  - Deletes rows in PostgreSQL that no longer exist in Google Sheets.

- **`/sync_gsheet`:** Synchronizes data from PostgreSQL to Google Sheets.
  - Fetches data from PostgreSQL.
  - Updates or inserts rows in Google Sheets based on the current state of the data in PostgreSQL.
  - Deletes rows in Google Sheets that no longer exist in PostgreSQL.

#### 2. **Real-Time Synchronization Using PostgreSQL Notifications**

To keep Google Sheets updated in real-time when changes occur in PostgreSQL:

- **Setup Triggers and Notifications:**
  - Created PostgreSQL triggers that call a function to notify changes using `pg_notify`.
  - Used a Python script to listen for these notifications and trigger the `/sync_gsheet` endpoint to update Google Sheets.

#### 3. **Handling Conflicts and Simultaneous Updates**


1. **Synchronization from Google Sheets to PostgreSQL:**
    - **Google Apps Script Trigger:** I created a Google Apps Script trigger that automatically detects changes in the Google Sheet. When changes are detected (such as adding, updating, or deleting rows), the script sends a POST request to our FastAPI `/sync_postgres` endpoint.
    - **FastAPI `/sync_postgres` Endpoint:** This endpoint processes the data received from Google Sheets. It compares the incoming data with the current data in PostgreSQL. New rows are inserted, and existing rows are updated. Rows that are present in PostgreSQL but not in Google Sheets are deleted. This ensures that PostgreSQL reflects the latest state of the Google Sheet.

2. **Synchronization from PostgreSQL to Google Sheets:**
    - **PostgreSQL Triggers and Notification Channel:** I set up PostgreSQL triggers to notify our FastAPI application whenever there are changes to the `leads` table. These triggers call a function that uses `pg_notify` to send notifications to a specified channel.
    - **FastAPI Notification Listener:** A background thread in FastAPI listens for these notifications. Upon receiving a notification, it triggers a POST request to the `/sync_gsheet` endpoint.
    - **FastAPI `/sync_gsheet` Endpoint:** This endpoint fetches the current data from PostgreSQL and compares it with the data in Google Sheets. It updates Google Sheets to reflect the changes in PostgreSQL. Rows that have been removed from PostgreSQL are deleted from Google Sheets, and rows that have been updated are updated accordingly.

3. **Synchronization from PostgreSQL to Google Sheets:**
  - **Google Sheets to PostgreSQL:** Conflicts are managed by the `ON CONFLICT` clause in the PostgreSQL `INSERT` statement. This clause updates existing records if there is a conflict on the primary key (`lead_id`), ensuring that updates from Google Sheets overwrite any existing data in PostgreSQL.

- **PostgreSQL to Google Sheets:** Updates in PostgreSQL trigger a comparison between the existing data in Google Sheets and PostgreSQL data. The FastAPI `/sync_gsheet` endpoint updates the Google Sheets data to match PostgreSQL, handling conflicts by ensuring that the latest data is always reflected.


#### 4. **Error Handling and Edge Cases**

- **Error Handling:**
  - Implemented error handling to manage issues with synchronization, such as network failures or data inconsistencies.

- **Edge Cases:**
  - Addressed scenarios with incomplete data, simultaneous updates, and data consistency.

This approach ensures a robust and reliable synchronization system that maintains data consistency between Google Sheets and PostgreSQL, even in the face of potential conflicts and edge cases.


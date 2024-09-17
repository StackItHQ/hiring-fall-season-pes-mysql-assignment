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

- [ X ] My code's working just fine! 🥳
- [ X ] I have recorded a video showing it working and embedded it in the README ▶️
- [ X ] I have tested all the normal working cases 😎
- [ ] I have even solved some edge cases (brownie points) 💪
- [ X ] I added my very planned-out approach to the problem at the end of this README 📜

## Got Questions❓
Feel free to check the discussions tab, you might get some help there. Check out that tab before reaching out to us. Also, did you know, the internet is a great place to explore? 😛

We're available at techhiring@superjoin.ai for all queries. 

All the best ✨.

## Developer's Section
*Add your video here, and your approach to the problem (optional). Leave some comments for us here if you want, we will be reading this :)*

## Demo Video

[Click here to watch the demo](https://drive.google.com/file/d/1vrxgiSn7zi-vl5pDFzRl1O6v-wp2w-UV/view?usp=sharing)

## Approach 

* First I figured out how to get the spreadSheet data using Api's and  and parse it .
* Dynamically I am creating and altering the database schema based on the columns in my sheet.
* And also updatating the new values in the database when the values gets changed in my sheet.
* Based on the  timestamp when the google sheet is updated the timestamp of google sheet is latest so i am updating it to database.
* And when the database is updated the time stamp of the db will be latest so will update the sheet.

## Challenges

* How to Synchronize between google sheet and database ,then figured out the google sheet api's to get data , timestamp and also updating the google sheet.
* Faced challenges to parse the json file got from google sheet api's ,it took some time to figure out how to parse the data.

## Steps to SetUp Google Sheet and Drive Api :

1) Create a Google Cloud Project:

- Go to Google Cloud Console.
- Create a new project.

2)Enable Google Sheets API:

- In the Google Cloud Console, go to APIs & Services > Library.
- Search for "Google Sheets API" and enable it for your project.

3) Create API Credentials:

- Go to APIs & Services > Credentials.
- Create a Service Account and download the JSON credentials file.

4) Enable Google Drive Api 

## SetUp the environment

```bash
python3 -m venv venv

source venv/bin/activate 
```

- the spreadsheet url is in the form : https://docs.google.com/spreadsheet_id/d/spreadsheed_id/edit?gid=0#gid=0
- Change the self.spreadsheet_id Variable in the __init__ method in DataSync_Gsheet_DB.py with the spreadsheet_id in url.
- Change the self.CREDENTIALS_FILE Variable in the __init__ method in DataSync_Gsheet_DB.py with path of your credentials JSON file.

- Change the configuration of database in the __init__ method in DataSync_Gsheet_DB.py
   - self.db = mysql.connector.connect(
            host='localhost',         # Database host
            user='yourusername',      # Your MySQL username
            password='yourpassword',  # Your MySQL password
            database='yourdatabase'   # Database name
        )

## Run the script
```bash
pip install -r requirements.txt

chmod +x ./schedule_script.sh
./schedule_script.sh

```





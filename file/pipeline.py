# Libraries import
import json
import pandas as pd
import requests
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Defined parameters
header_file = '/Users/user/Documents/Linkedln data project/config/header.json'
url = "https://linkedin-data-scraper.p.rapidapi.com/search_jobs"
logfile = r'/Users/user/Documents/Linkedln data project/logs/log.txt'

# Database Credentials
load_dotenv()

host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')

db_details = {
    'host': host,
    'user': user,
    'password': password,
    'database': database
}

# Loading my header file that contains my API key
with open(header_file, 'r') as file:
    header = json.load(file)

# logging function
def logging(message, log_print=False):
    timeformat = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    time = now.strftime(timeformat)
    
    with open(logfile, 'a') as log:
        log.write(f"{time} - {message}\n")
        
    if log_print:
        print(f"{time} - {message}")

# Extract function
def extract_data(url):
    payload = {
        "keywords": "Data Engineer OR Data Scientist OR Data Analyst",
        "location": "California, United States",
        "count": 100
    }
    
    response = requests.post(url, json=payload, headers=header)
    data = response.json()['response']
    return data

# Transform function
def transform(data):
    main = []

    for i in data:
        main.extend(i)  

    df = pd.DataFrame(main)
    logging('Began the renaming of columns', log_print=True)
    
    # column names before renaming
    logging(str(list(df.columns)), log_print=True)

    # Renaming columns
    column_to_rename = {
        'title': 'JobTitle',
        'comapnyURL1': 'CompanyURL1',
        'comapnyURL2': 'CompanyURL2',
        'companyId': 'CompanyId',
        'companyUniversalName': 'CompanyUniversalName',
        'companyName': 'CompanyName',
        'salaryInsights': 'SalaryInsights',
        'applicants': 'NoOfApplicants',
        'formattedLocation': 'CompanyLocation',
        'formattedEmploymentStatus': 'EmploymentStatus',
        'formattedExperienceLevel': 'ExperienceLevel',
        'formattedIndustries': 'Industries',
        'jobDescription': 'JobDescription',
        'inferredBenefits': 'Benefits',
        'jobFunctions': 'JobFunctions',
        'companyApplyUrl': 'CompanyApplicationUrl',
        'jobPostingUrl': 'JobPostingUrl',
        'listedAt': 'PostedDate'
    }

    df.rename(columns=column_to_rename, inplace=True)
    logging('Completed the task of renaming columns', log_print=True)

     # column names after renaming
    logging(str(list(df.columns)), log_print=True)

    # Filling null values if present
    logging('Began the task of replacing null values', log_print=True)
    # Check for NaN values and fill with 'Unknown'
    columns_to_fill = [
        'JobTitle', 'CompanyURL1', 'CompanyURL2', 'CompanyId', 'CompanyUniversalName',
        'CompanyName', 'SalaryInsights', 'NoOfApplicants', 'CompanyLocation',
        'EmploymentStatus', 'ExperienceLevel', 'Industries', 'JobDescription',
        'Benefits', 'JobFunctions', 'CompanyApplicationUrl', 'JobPostingUrl', 'PostedDate'
    ]
    for col in columns_to_fill:
        if col in df.columns:
            df[col].fillna('Unknown', inplace=True)
    logging('Completed the task of replacing null values', log_print=True)

    # Removing rows with 'Unknown' values in specific columns
    df = df[(df['JobTitle'] != 'Unknown') & (df['CompanyName'] != 'Unknown') & (df['JobPostingUrl'] != 'Unknown')]

    # Data types before conversion
    logging(str(df.dtypes), log_print=True)

    # Changing datatype from object to numeric
    columns_to_change = ['CompanyId', 'SalaryInsights', 'NoOfApplicants']
    for col in columns_to_change:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Changing datatype from object to datetime
    df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')

    # Data types after conversion
    logging(str(df.dtypes), log_print=True)

    # Dropping unnecessary columns
    columns_to_drop = ['CompanyURL1', 'CompanyUniversalName', 'JobFunctions', 'CompanyApplicationUrl', 'JobDescription']
    df.drop(columns=columns_to_drop, inplace=True)
    logging(str(df.head(2)), log_print=True)

    # Saving result to CSV
    try:
        logging('Preparing to save the data', log_print=True)
        filename = f"Linkedln_Data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging('Data Saved to csv', log_print=True)
    except Exception as e:
        logging(f"Failed to save the data because of this error: {e}", log_print=True)
    
    return df

# connection function
def connection():
    try:
        logging('Making attempt to connect to the database', log_print=True)
        mydb = mysql.connector.connect(**db_details)
        logging('Database connection successful', log_print=True)
    except Exception as e:
        logging(f"Connection failed because of this error: {e}", log_print=True)
        mydb = None
    return mydb

# execute_query function
def execute_query(query):
    try:
        logging('Making attempt to connect to the database to execute a query', log_print=True)
        mydb = connection()
        if mydb:
            mycursor = mydb.cursor()
            try:
                logging('Executing SQL Query', log_print=True)
                mycursor.execute(query)
                mydb.commit()
                logging('Query Executed Successfully', log_print=True)
            except Exception as e:
                logging(f"Failed to execute query because of this error: {e}", log_print=True)
            finally:
                mycursor.close()
                mydb.close()
    except Exception as e:
        logging(f"Connection failed because of this error: {e}", log_print=True)

# upload_query function
def upload_data(dataframe, table, upload_type='append'):
    try:
        logging('Making attempt to connect to the database to upload data', log_print=True)
        mydb = connection()
        if mydb:
            mycursor = mydb.cursor()
            try:
                logging('Attempting to upload the data', log_print=True)
                dataframe.to_sql(
                    name=table,
                    con=create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"),
                    if_exists=upload_type,
                    index=False
                )
                logging('Data uploaded successfully', log_print=True)
            except Exception as e:
                logging(f"Failed to upload the data into the database because of this error: {e}", log_print=True)
            finally:
                mycursor.close()
                mydb.close()
    except Exception as e:
        logging(f"Connection failed because of this error: {e}", log_print=True)

# Logging my ETL information
logging('ETL Job Started', log_print=True)

try:
    logging('Extract Task Started', log_print=True)
    extract_task = extract_data(url)
    logging('Extract Task Ended', log_print=True)

    logging('Transform Task Started', log_print=True)
    transform_task = transform(extract_task)
    logging('Transform Task Ended', log_print=True)

    logging('Creating job table in my database', log_print=True)
    query = """
        CREATE TABLE IF NOT EXISTS Jobs (
        id INT PRIMARY KEY AUTO_INCREMENT,
        JobTitle VARCHAR(255),
        CompanyURL2 VARCHAR(255),
        CompanyId INT,
        CompanyName VARCHAR(255),
        SalaryInsights VARCHAR(255),
        NoOfApplicants INT,
        CompanyLocation VARCHAR(255),
        EmploymentStatus VARCHAR(255),
        ExperienceLevel VARCHAR(255),
        Industries VARCHAR(255),
        Benefits TEXT,
        JobPostingUrl VARCHAR(255),
        PostedDate DATE
    );
    """
    execute_query(query)
    logging('Created the job table in my database', log_print=True)

    table = 'Jobs'
    upload_data(transform_task, table)

    logging('ETL Task Completed. So, I fit go sleep. Lol', log_print=True)
except Exception as e:
    logging(f'Omo, error dey one place ooh, go debug. This na why e fail: {e}', log_print=True)

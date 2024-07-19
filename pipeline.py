#%%
# Libraries import
import json
import pandas as pd
import requests
from datetime import datetime
import os
from dotenv import load_dotenv


#%%
# Defined parameters
header_file = 'config/config.json'
url = "https://linkedin-data-scraper.p.rapidapi.com/search_jobs"
logfile = 'logs/log.txt'

#%%
# Loading my header file that contains my API key
with open(header_file, 'r') as file:
    header = json.load(file)

#%%
# Logging function
def logging(message, log_print=False):
    timeformat = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    time = now.strftime(timeformat)
    
    with open(logfile, 'a') as log:
        log.write(f"{time} - {message}\n")
        
    if log_print:
        print(f"{time} - {message}")

#%%
# Extract function
def extract_data(url):
    payload = {
        "keywords": "Data Engineer",
        "location": "California, United States",
        "count": 100
    }
    
    response = requests.post(url, json=payload, headers=header)
    data = response.json()['response']
    return data

#%%
# Transform function
def transform(data):
    main = []

    for i in data:
        main.extend(i)  

    df = pd.DataFrame(main)
    logging('Began the renaming of columns', log_print=True)
    

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
    print('Completed the task of renaming columns')


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


    # Changing datatype from object to numeric
    columns_to_change = ['CompanyId', 'SalaryInsights', 'NoOfApplicants']
    for col in columns_to_change:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Changing datatype from object to datetime
    df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')

    # Dropping unnecessary columns
    columns_to_drop = ['CompanyURL1', 'CompanyUniversalName', 'JobFunctions', 'CompanyApplicationUrl', 'JobDescription']
    df.drop(columns=columns_to_drop, inplace=True)

    # Saving result to CSV
    try:
        logging('Preparing to save the data', log_print=True)
        filename = f"file/Linkedln_Data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logging('Data Saved to csv', log_print=True)
    except Exception as e:
        logging(f"Failed to save the data because of this error: {e}", log_print=True)
    
    return df

#%%
# Logging my ETL information
logging('ETL Job Started', log_print=True)

#%%
try:
    logging('Extract Task Started', log_print=True)
    extract_task = extract_data(url)
    logging('Extract Task Ended', log_print=True)

    logging('Transform Task Started', log_print=True)
    transform_task = transform(extract_task)
    logging('Transform Task Ended', log_print=True)

    logging('ETL Task Completed. So, I fit go sleep. Lol', log_print=True)
except Exception as e:
    logging(f'Omo, error dey one place ooh, go debug. This na why e fail: {e}', log_print=True)

# %%

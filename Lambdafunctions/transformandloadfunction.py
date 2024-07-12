import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name = event['Records'][0]['s3']['object']['key']
        
        destination_bucket = 'cleanedlinkedlndata'
        target_file = file_name[:-5]
        
        wait_time = s3_client.get_waiter('object_exists')
        wait_time.wait(Bucket=bucket_name, Key=file_name)
        
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        print('S3 Object Retrieved:', response)
        
        raw_data = response['Body'].read().decode('utf-8')
        data = json.loads(raw_data)
        
        main = []
        for i in data:
            main.extend(i)  

        df = pd.DataFrame(main)
        
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
        
        # Print column data types before conversion
        print('Column data types before conversion:\n', df.dtypes)
        
        # Changing datatype from object to numeric
        columns_to_change = ['CompanyId', 'SalaryInsights', 'NoOfApplicants']
        for col in columns_to_change:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Changing datatype from object to datetime
        df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')
        
        # Print column data types after conversion
        print('Column data types after conversion:\n', df.dtypes)
        
        # Dropping unnecessary columns
        columns_to_drop = ['CompanyURL1', 'CompanyUniversalName', 'JobFunctions', 'CompanyApplicationUrl', 'JobDescription']
        df.drop(columns=columns_to_drop, inplace=True)
        
        print('Transformed DataFrame:\n', df)
        
        # Saving to CSV
        data = df.to_csv(index=False)
        cleaned_file_name = f'{target_file}.csv'
        
        # Inserting the data into the bucket
        s3_client.put_object(Bucket=destination_bucket, Key=cleaned_file_name, Body=data)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data Transformed and loaded into the bucket')
        }
    except Exception as e:
        print('Error processing S3 event:', e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing S3 event')
        }

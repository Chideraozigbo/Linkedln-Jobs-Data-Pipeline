# LinkedIn Data ETL Pipeline

This project involves extracting job listing data from the LinkedIn API, processing and storing it in S3 buckets, and ultimately loading it into a Redshift data warehouse using AWS Lambda and Apache Airflow. Additionally, a copy of the processed data is sent via email.

## Architecture Overview

1. **Data Extraction**: Data is extracted from the LinkedIn API based on specified job keywords and location.
2. **Landing Zone Storage**: The extracted data is initially stored in a landing zone S3 bucket.
3. **Intermediate Storage**: An AWS Lambda function is triggered, which copies the data from the landing zone bucket to an intermediate S3 bucket.
4. **Data Transformation**: Another AWS Lambda function is triggered, which retrieves the data from the intermediate bucket, transforms it, and loads it into a cleaned S3 bucket.
5. **Data Warehouse Loading**: Airflow senses the new data in the cleaned bucket and loads it into a Redshift data warehouse.
6. **Email Notification**: A copy of the cleaned data is sent via email.

## Detailed Flow

1. **Extract Data**:
    - Data is fetched from the LinkedIn API using specified parameters (e.g., job titles and location).
    - The raw data is saved as a JSON file and a CSV file on the local file system.

2. **Landing Zone Storage**:
    - The JSON file is uploaded to the landing zone S3 bucket.

3. **Intermediate Storage**:
    - An AWS Lambda function is triggered upon the arrival of new data in the landing zone bucket.
    - The Lambda function copies the data to an intermediate S3 bucket for further processing.

4. **Data Transformation**:
    - A second Lambda function is triggered by the presence of new data in the intermediate bucket.
    - This function processes and transforms the data, then stores the cleaned data in the cleaned S3 bucket.

5. **Data Warehouse Loading**:
    - An Airflow DAG with an S3 sensor task monitors the cleaned bucket for new data.
    - Once new data is detected, it is loaded into a Redshift data warehouse for analysis and reporting.

6. **Email Notification**:
    - The cleaned data is also sent via email to the specified recipient as a CSV attachment.

## Architecture Diagram

![Architecture Diagram](airflow/architecture.png)

## Dependencies

- **Python Libraries**:
    - `airflow`
    - `pandas`
    - `boto3`
    - `requests`

- **AWS Services**:
    - S3
    - Lambda
    - Redshift

## Configuration

- API key and other configurations are stored in a JSON file (`config.json`).
- Ensure your AWS credentials are properly configured for the `boto3` library.

## Airflow DAG

- The DAG is configured to run daily, starting from July 7, 2024.
- Tasks include data extraction, file processing, and email notification.

## Lambda Functions

- Ensure your Lambda functions are properly set up with the necessary permissions to access the S3 buckets and trigger based on S3 events.

## Documentation

For the documentation of this project on medium, Kindly have a read here [Medium Documentation](https://medium.com/@chideraozigbo/linkedin-jobs-data-etl-pipeline-190e10810fa1)

## Contact

For any queries, please contact `chideraozigbo@gmail.com`.

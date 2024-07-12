# Libraries import
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.email import send_email_smtp
import pandas as pd
import json
import requests
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
import boto3
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Defined parameters
header_file = '/home/ubuntu/airflow/config/config.json'

# Defined bucket name
s3_bucket = 'cleanedlinkedlndata'

# Loading my header file that contains my api key
with open(header_file, 'r') as file:
    header = json.load(file)

def extract_data(ti):
    url = "https://linkedin-data-scraper.p.rapidapi.com/search_jobs"

    payload = {
        "keywords": "Data Engineer OR Data Scientist OR Data Analyst",
        "location": "California, United States",
        "count": 100
    }

    response = requests.post(url, json=payload, headers=header)
    data = response.json()
    data = data['response']
    print(data)

    time_format = '%Y%m%d%H%M%S'
    now = datetime.now()
    time_stamp = now.strftime(time_format)

    file_path = f'/home/ubuntu/linkedln_data_{time_stamp}.json'
    file_name = f'linkedln_data_{time_stamp}.csv'
    file_name1 = '/home/ubuntu/airflow/dags/files/linkedln_data.csv'

    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    df = pd.DataFrame(data)
    df.to_csv(file_name1, index=False)

    output_file = [file_path, file_name]
    return output_file



default_args = {
    'owner': 'Chidera',
    'email': ['chideraozigbo@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

with DAG('linkedln_dag', default_args=default_args, start_date=datetime(2024, 7, 7), schedule_interval = '*/10 * * * *', catchup=False) as linkedln_dag:

    extract_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data
    )

    with TaskGroup("file_processing", tooltip="File Processing tasks") as file_processing:
        file_sensor_task = FileSensor(
            task_id='file_sensor',
            filepath='/home/ubuntu/linkedln_data_*.json',
            poke_interval=300,
            timeout=600
        )

        load_Data_from_EC2_into_S3 = BashOperator(
            task_id='load_Data_from_EC2_into_S3',
            bash_command="aws s3 mv {{ ti.xcom_pull(task_ids='extract_data_task')[0] }} s3://linkedlndatalandingzone/",
            retries=3
        )

        s3_file_sensor = S3KeySensor(
            task_id='s3_key_sensor_task',
            bucket_name=s3_bucket,
            bucket_key="{{ ti.xcom_pull(task_ids='extract_data_task')[1] }}",
            aws_conn_id='aws_default',
            timeout=60,
            poke_interval=5,
            wildcard_match=False
        )

        load_from_s3_to_redshift = S3ToRedshiftOperator(
        task_id="s3_to_redshift_task",
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_default',
        s3_bucket=s3_bucket,
        s3_key="{{ ti.xcom_pull(task_ids='extract_data_task')[1] }}",
        schema="public",
        table="Jobs",
        copy_options=[
        "csv",
        "IGNOREHEADER 1",
        "NULL AS ''",
        "TIMEFORMAT 'auto'",
        "MAXERROR 100",
        "EMPTYASNULL",
        "BLANKSASNULL"
         ],
        )

        file_sensor_task >> load_Data_from_EC2_into_S3 >> s3_file_sensor >> load_from_s3_to_redshift

    email_task = EmailOperator(
        task_id='send_email',
        to='fegidorenaissanceclassof18@gmail.com',
        subject='LinkedIn Data CSV File',
        html_content='Please find the attached CSV file containing LinkedIn job data.',
        files=['/home/ubuntu/airflow/dags/files/linkedln_data.csv'],
        dag=linkedln_dag
    )

    extract_task >> file_processing >> email_task

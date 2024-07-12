import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    
    destination_bucket = 'linkedlndataintermediate'
    copy_from = {'Bucket': bucket_name, 'Key': file_name}
    
    wait_time = s3_client.get_waiter('object_exists')
    wait_time.wait(Bucket=bucket_name, Key=file_name)
    s3_client.copy_object(Bucket=destination_bucket, Key=file_name, CopySource=copy_from)
    return {
        'statusCode': 200,
        'body': json.dumps('Data copied and loaded into the intermediate Bucket')
    }

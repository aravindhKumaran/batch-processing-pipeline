import boto3
import json
import os
import subprocess
from datetime import datetime, timedelta
from send_email import send_email


def lambda_handler(event, context):

    BUCKET_NAME = os.environ.get('BUCKET_NAME')
    PUBLIC_DNS = os.environ.get('PUBLIC_DNS')

    datestr = (datetime.today()).strftime("%Y%m%-d")
    print(f"todays date is : {datestr}")
    
    #create s3 client
    s3_client=boto3.client('s3')    
    
    s3_file_list = []
    for object in s3_client.list_objects_v2(Bucket= BUCKET_NAME)['Contents']:
        s3_file_list.append(object['Key'])
    print("s3_file_list:", s3_file_list)
    

    required_file_list = [
        f'calendar_{datestr}.csv.gz', 
        f'inventory_{datestr}.csv.gz', 
        f'product_{datestr}.csv.gz', 
        f'sales_{datestr}.csv.gz', 
        f'store_{datestr}.csv.gz',

        ]

    print("required_file_list:", required_file_list)

    selected_files = [file for file in s3_file_list if file in required_file_list]     
    if len(selected_files) < len(required_file_list):
        print("Email sent")
        send_email()
    else:
        s3_file_url = ['s3://' + 'retail_data_raw/' + a for a in selected_files]
        table_name = [a[:-15] for a in selected_files]   
        print(s3_file_url, table_name)
                    
        data_dict = dict(zip(table_name, s3_file_url))
        conf_dict = {"conf": data_dict}
                    
        data = json.dumps(conf_dict) 
        print(data)
                
        # send signal to Airflow    
        endpoint= F'http://{PUBLIC_DNS}:8080/api/v1/dags/batch_processing_dag/dagRuns'
        
        subprocess.run(['curl', '-X', 'POST', endpoint, '-H', 'Content-Type: application/json','--user', 'airflow:airflow', '-d', data])
        print('File are send to Airflow')    
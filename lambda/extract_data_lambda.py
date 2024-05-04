import boto3
import pymysql
import csv
import io
import os
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
rds_host = "retail_database.9847238345.ca-central-1.rds.amazonaws.com"
username = "root"
database_name = "retail_database"
s3_bucket_name = "retail_data_raw"

def fetch_data_from_rds(table_name, password):
    # Connect to RDS
    conn = pymysql.connect(host=rds_host, user=username, passwd=password, db=database_name, connect_timeout=5)
    
    # Execute SQL query
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
    
    return rows

def lambda_handler(event, context):

    db_password = os.getenv("DB_PASSWORD")

    # Get current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Tables to fetch data from
    tables = ['calendar', 'inventory', 'product', 'sales', 'store']
    
    # Fetch data from each table and upload to S3
    s3_client = boto3.client('s3')
    for table in tables:
        table_data = fetch_data_from_rds(table)
        
        # Write data to CSV in memory
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerows(table_data)
        
        # Upload CSV to S3 with specified key
        s3_key = f"{table}_{current_date}.csv.gz"
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': 'Data loaded to S3 successfully'
    }

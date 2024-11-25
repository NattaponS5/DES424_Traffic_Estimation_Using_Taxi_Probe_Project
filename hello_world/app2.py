import json
import pandas as pd
import requests
import io  # For working with in-memory file-like objects
import boto3   # For S3 interaction (if needed)

# Configure boto3 with your AWS credentials and region
boto3.setup_default_session(
    aws_access_key_id='XXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
    aws_session_token='XXXXXXXXXXXXXXXXXXX',
    region_name='us-east-1'
)

s3 = boto3.client('s3') # Initialize boto3 s3 client

def lambda_handler():
    # Load data from S3 (adjust bucket and key)
    bucket_name = 'test-taxi-20180801-5min' # Replace with your S3 bucket
    file_key = 'mid_taxi_20180801_0000.csv.csv'  # Replace with your data's key
    
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj['Body'].read().decode('utf-8')  # Decode if necessary
        df = pd.read_csv(io.StringIO(csv_data)) # Make it work in memory without saving locally
        
        # Show the first 20 rows
        print(df.head(20))
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500, 
            'body': json.dumps({'error': 'Failed to load data from S3'}) # Return an error response
        }
import boto3
import csv
from io import StringIO
import concurrent.futures

boto3.setup_default_session(
    aws_access_key_id='XXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
    aws_session_token='XXXXXXXXXXXXXXXXXXX',
    region_name='us-east-1'
)

s3 = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')

bucket_name = "taxi-20180801-5min"
# csv_files_to_migrate = ['final_taxi_20180801_0700.csv']
# csv_files_to_migrate = ['final_taxi_20180801_0705.csv', 'final_taxi_20180801_0710.csv', 'final_taxi_20180801_0715.csv']
csv_files_to_migrate = ['final_taxi_20180801_0720.csv', 'final_taxi_20180801_0725.csv', 'final_taxi_20180801_0730.csv', 'final_taxi_20180801_0735.csv', 'final_taxi_20180801_0740.csv', 'final_taxi_20180801_0745.csv', 'final_taxi_20180801_0750.csv', 'final_taxi_20180801_0755.csv']

def create_dynamodb_table():
    try:
        table = dynamodb.create_table(
            TableName='taxi-20180801-5min',
            KeySchema=[
                {
                    'AttributeName': 'way_info_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'way_info_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='taxi-20180801-5min')
        print(f"Table {table.table_name} created successfully.")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        print("Table already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

def upload_to_dynamodb(file_name):
    try:
        table = dynamodb.Table('taxi-20180801-5min')
        # Check if the table exists by describing it
        table.load()
        print(f"Successfully accessed table: {table.table_name}")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print("Table not found.")
        return
    except Exception as e:
        print(f"Error accessing table: {e}")
        return
    
    # Extract timestamp from file name
    timestamp = file_name.split('_')[-1].split('.')[0]
    
    # Download the file from S3
    csv_file = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = csv_file['Body'].read().decode('utf-8')
    
    # Read the CSV content
    csv_reader = csv.DictReader(StringIO(csv_content))
    
    with table.batch_writer() as batch:
        for row in csv_reader:
            # Ensure the primary key attributes are present
            try:
                if 'way_info_id' in row:
                    row['timestamp'] = timestamp
                    # Remove decimal points from specified columns
                    row['way_info_id'] = row['way_info_id'].split('.')[0]
                    row['average_speed'] = row['average_speed'].split('.')[0]
                    row['speedlimit'] = row['speedlimit'].split('.')[0]
                    batch.put_item(Item=row)
                else:
                    print(f"Missing primary key attributes in row: {row}")
            except Exception as e:
                print(f"Error uploading row {row}: {e}")

def lambda_handler():
    try:
        table = dynamodb.Table('taxi-20180801-5min')
        table.load()  # Check if the table exists by loading it
        print(f"Table {table.table_name} already exists.")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print("Table not found. Creating table.")
        create_dynamodb_table()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(upload_to_dynamodb, file) for file in csv_files_to_migrate]
        concurrent.futures.wait(futures)
    return {
        'statusCode': 200,
        'body': 'Data migration completed successfully'
    }
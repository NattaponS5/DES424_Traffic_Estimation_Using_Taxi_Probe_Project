# access data from dynamodb
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr

boto3.setup_default_session(
    aws_access_key_id='XXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
    aws_session_token='XXXXXXXXXXXXXXXXXXX',
    region_name='us-east-1'
)

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def query_dynamodb(table_name, timestamp=None, way_info_id=None):
    table = dynamodb.Table(table_name)
    
    if timestamp and way_info_id:
        # Query with both attributes
        key_condition_expression = Key('timestamp').eq(timestamp) & Key('way_info_id').eq(way_info_id)
        response = table.query(
            KeyConditionExpression=key_condition_expression
        )
        return response['Items']
    elif timestamp:
        # Scan with only timestamp
        response = table.scan(
            FilterExpression=Attr('timestamp').eq(timestamp)
        )
        return response['Items']
    elif way_info_id:
        # Scan with only way_info_id
        response = table.scan(
            FilterExpression=Attr('way_info_id').eq(way_info_id)
        )
        return response['Items']
    else:
        return []

# Example usage
table_name = 'taxi-20180801-5min'
timestamp = '0700'
way_info_id = None # None if you only want to query by timestamp

items = query_dynamodb(table_name, timestamp, way_info_id)

# Convert the items to a pandas DataFrame and print it
df = pd.DataFrame(items)
print(df)
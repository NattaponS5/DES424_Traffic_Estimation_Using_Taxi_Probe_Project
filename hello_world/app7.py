import boto3
import requests
import pandas as pd
import folium
import time
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import zipfile
import io

# Specify your AWS credentials
aws_access_key_id='XXXXXXXXXXXXXXXXX',
aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
aws_session_token='XXXXXXXXXXXXXXXXXXX',
region_name='us-east-1'

# Connect to DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
table = dynamodb.Table('taxi-20180801-5min')

# DynamoDB table name
TABLE_NAME = "taxi-20180801-5min"

# Overpass API endpoint
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Define chunk size
CHUNK_SIZE = 50

# S3 bucket and key for storing the HTML file
S3_BUCKET_NAME = 'genmap-app'
S3_KEY = 'thailand_roads_map_rotate.html'

# Amplify App ID and Branch Name
AMPLIFY_APP_ID = 'd2xz524t6c5f8p'
AMPLIFY_BRANCH_NAME = 'main'

def query_overpass_chunk(way_ids_chunk):
    """
    Query the Overpass API with a chunk of way IDs.
    """
    way_ids_str = ','.join(map(str, way_ids_chunk))
    query = f"""
    [out:json];
    way(id:{way_ids_str});
    out geom;
    """
    try:
        response = requests.get(OVERPASS_URL, params={'data': query.strip()})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying Overpass API: {e}")
        return None

def fetch_data_from_dynamodb(timestamp):
    """
    Fetch data from DynamoDB filtered by a specific timestamp.
    """
    response = table.scan(
        FilterExpression=Attr("timestamp").eq(timestamp)
    )
    items = response.get('Items', [])

    # Convert to a DataFrame
    df = pd.DataFrame(items)
    df['way_info_id'] = pd.to_numeric(df['way_info_id'], errors='coerce')
    df['average_speed'] = pd.to_numeric(df['average_speed'], errors='coerce')
    df['speedlimit'] = pd.to_numeric(df['speedlimit'], errors='coerce')

    return df

def generate_map_html(traffic_data, timestamp):
    """
    Generate an HTML map based on traffic data.
    """
    map_center = [13.7563, 100.5018]
    m = folium.Map(location=map_center, zoom_start=12)

    way_ids = traffic_data['way_info_id'].astype(int).tolist()
    for i in range(0, len(way_ids), CHUNK_SIZE):
        way_ids_chunk = way_ids[i:i + CHUNK_SIZE]
        result = query_overpass_chunk(way_ids_chunk)

        if result and 'elements' in result:
            for way in result['elements']:
                if way['type'] == 'way' and 'geometry' in way:
                    coords = [(node['lat'], node['lon']) for node in way['geometry']]
                    way_id = way['id']

                    row = traffic_data.loc[traffic_data['way_info_id'] == way_id]
                    color = row['color'].values[0]
                    tags_name = row['tags_name_en'].values[0] if 'tags_name_en' in row else "Unknown"

                    folium.PolyLine(
                        locations=coords,
                        color=color,
                        weight=5,
                        tooltip=f"Way ID: {way_id}, Tags: {tags_name}"
                    ).add_to(m)

    # Save the map to a string
    html_content = m.get_root().render()
    return html_content

def create_zip_archive(html_content):
    """
    Create a ZIP archive containing the HTML content.
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr('index.html', html_content)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def upload_to_s3(zip_content):
    """
    Upload the ZIP content to S3.
    """
    s3 = boto3.client('s3',
                      region_name='us-east-1',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token)

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_KEY,
        Body=zip_content,
        ContentType='application/zip'
    )

def get_presigned_url():
    """
    Get a pre-signed URL for the uploaded S3 object.
    """
    s3 = boto3.client('s3',
                      region_name='us-east-1',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token)

    presigned_url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET_NAME, 'Key': S3_KEY},
        ExpiresIn=3600  # URL expires in 1 hour
    )
    print(f"Presigned URL: {presigned_url}")

    return presigned_url

def trigger_amplify_deployment():
    """
    Trigger an Amplify deployment.
    """
    amplify = boto3.client('amplify',
                           region_name='us-east-1',
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           aws_session_token=aws_session_token)

    source_url = get_presigned_url()

    try:
        response = amplify.start_deployment(
            appId=AMPLIFY_APP_ID,
            branchName=AMPLIFY_BRANCH_NAME,
            sourceUrl=source_url
        )
        print(f"Deployment triggered successfully: {response}")
    except ClientError as e:
        print(f"Error triggering Amplify deployment: {e}")

def create_amplify_branch():
    """
    Create a new branch in Amplify if it doesn't exist.
    """
    amplify = boto3.client('amplify',
                           region_name='us-east-1',
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           aws_session_token=aws_session_token)

    try:
        response = amplify.create_branch(
            appId=AMPLIFY_APP_ID,
            branchName=AMPLIFY_BRANCH_NAME
        )
        print(f"Branch '{AMPLIFY_BRANCH_NAME}' created successfully: {response}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BadRequestException':
            print(f"Branch '{AMPLIFY_BRANCH_NAME}' already exists.")
        else:
            print(f"Error creating Amplify branch: {e}")

def lambda_handler():
    """
    Main function to update the map every 10 seconds cycling through timestamps.
    """
    # Create the branch if it doesn't exist
    create_amplify_branch()

    # List of timestamps to loop through
    timestamps = [f"07{str(i).zfill(2)}" for i in range(5, 60, 5)]  # ['0705', '0710', ..., '0755']
    current_index = 0

    while True:
        # Get the current timestamp
        current_timestamp = timestamps[current_index]

        # Fetch data for the current timestamp
        traffic_data = fetch_data_from_dynamodb(current_timestamp)

        if not traffic_data.empty:
            html_content = generate_map_html(traffic_data, current_timestamp)
            zip_content = create_zip_archive(html_content)
            upload_to_s3(zip_content)
            trigger_amplify_deployment()
            print(f"Map updated successfully for timestamp {current_timestamp}")
        else:
            print(f"No data found for timestamp {current_timestamp}")

        # Move to the next timestamp (circularly)
        current_index = (current_index + 1) % len(timestamps)

        # Wait for 10 seconds before updating again
        time.sleep(10)

# Entry point
if __name__ == "__main__":
    lambda_handler()

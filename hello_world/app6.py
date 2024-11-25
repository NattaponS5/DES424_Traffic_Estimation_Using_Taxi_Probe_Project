import boto3
import pandas as pd
import folium
import requests


# Specify your AWS credentials
aws_access_key_id='XXXXXXXXXXXXXXXXX',
aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
aws_session_token='XXXXXXXXXXXXXXXXXXX',
region_name='us-east-1'

# Connect to DynamoDB with credentials
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
table = dynamodb.Table('taxi-20180801-5min')
# DynamoDB table name
TABLE_NAME = "taxi-20180801-5min"

# Overpass API endpoint
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Define chunk size
CHUNK_SIZE = 50


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
        # Send the request
        response = requests.get(OVERPASS_URL, params={'data': query.strip()})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying Overpass API: {e}")
        return None


def fetch_data_from_dynamodb():
    """
    Fetch data from DynamoDB and convert it to a DataFrame.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    # Scan the table to fetch all items (caution with large tables!)
    response = table.scan()
    items = response.get('Items', [])

    # Convert to a DataFrame
    df = pd.DataFrame(items)

    # Ensure numeric columns are correctly cast (if needed)
    df['way_info_id'] = pd.to_numeric(df['way_info_id'], errors='coerce')
    df['average_speed'] = pd.to_numeric(df['average_speed'], errors='coerce')
    df['speedlimit'] = pd.to_numeric(df['speedlimit'], errors='coerce')

    return df


def lambda_handler():
    """
    AWS Lambda handler function.
    """
    # Fetch data from DynamoDB
    traffic_data = fetch_data_from_dynamodb()

    # Initialize a Folium map centered on Bangkok
    map_center = [13.7563, 100.5018]
    m = folium.Map(location=map_center, zoom_start=12)

    # Process way IDs in chunks
    way_ids = traffic_data['way_info_id'].astype(int).tolist()  # Ensure way IDs are integers
    for i in range(0, len(way_ids), CHUNK_SIZE):
        way_ids_chunk = way_ids[i:i + CHUNK_SIZE]
        result = query_overpass_chunk(way_ids_chunk)

        if result and 'elements' in result:
            for way in result['elements']:
                if way['type'] == 'way' and 'geometry' in way:
                    coords = [(node['lat'], node['lon']) for node in way['geometry']]
                    way_id = way['id']
                    
                    # Find the corresponding color and tags from the DataFrame
                    row = traffic_data.loc[traffic_data['way_info_id'] == way_id]
                    color = row['color'].values[0]
                    tags_name = row['tags_name_en'].values[0] if 'tags_name_en' in row else "Unknown"

                    folium.PolyLine(
                        locations=coords,
                        color=color,
                        weight=5,
                        tooltip=f"Way ID: {way_id}, Tags: {tags_name}"
                    ).add_to(m)

    # Save the map as an HTML file in /tmp
    output_file_path = "../tmp/thailand_roads_map.html"
    m.save(output_file_path)

    # Return success response
    return {
        "statusCode": 200,
        "body": "Map generated successfully!",
        "map_path": output_file_path
    }
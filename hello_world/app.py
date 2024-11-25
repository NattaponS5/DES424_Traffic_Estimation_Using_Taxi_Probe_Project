import json
import pandas as pd
import asyncio
import aiohttp
import io
import boto3
from collections import defaultdict

# Configure boto3 with your AWS credentials and region
boto3.setup_default_session(
    aws_access_key_id='XXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXX',
    aws_session_token='XXXXXXXXXXXXXXXXXXX',
    region_name='us-east-1'
)

s3 = boto3.client('s3')

def lambda_handler():
    # Load data from S3 (adjust bucket and key)
    bucket_name = 'taxi-20180801-5min'
    file_key = 'taxi_20180801_0905.csv'
    
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_data))
        df = df.head(400)  # Limit to 3000 rows for testing
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to load data from S3'})
        }

    batch_size = 20  # Adjust batch size for optimal performance

    ecs_api_urls = [
        "http://3.92.187.175:5000/find_nearest_node",
        "http://52.87.3.107:5000/find_nearest_node"
    ]

    headers = {'Content-Type': 'application/json'}

    new_columns = ["nearest_file", "nearest_node_id", "way_info_id", "highway_type", "tags_name_th", "tags_name_en", "speedlimit"]
    for column in new_columns:
        df[column] = None

    cache = defaultdict(dict)

    async def process_batch(session, start_index, end_index, ecs_api_url, semaphore):
        async with semaphore:
            batch_data = []
            for _, row in df.iloc[start_index:end_index].iterrows():
                coord = (row['Lat'], row['Lon'])
                if coord in cache:
                    result = cache[coord]
                    original_df_index = start_index + len(batch_data)
                    for column, value in result.items():
                        df.loc[original_df_index, column] = value
                    continue
                batch_data.append({"point_lat": row["Lat"], "point_lon": row["Lon"]})

            payload = {"points": batch_data}
            print(f"Processing rows {start_index} to {end_index - 1} with {ecs_api_url}...")
            try:
                async with session.post(ecs_api_url, json=payload, headers=headers) as response:
                    response.raise_for_status()
                    results = await response.json()
                    for j, result in enumerate(results):
                        original_df_index = start_index + j
                        if result.get('nearest_file', '') == '':
                            print(f"Skipping row {original_df_index} due to empty nearest_file.")
                            continue

                        way_tags = result.get("waytags", {})

                        df.loc[original_df_index, 'nearest_file'] = result.get("nearest_file", None)
                        df.loc[original_df_index, 'nearest_node_id'] = result.get("nearest_node", {}).get("id", None)
                        df.loc[original_df_index, 'way_info_id'] = result.get("way_id", None)
                        df.loc[original_df_index, 'highway_type'] = way_tags.get("highway", None)
                        df.loc[original_df_index, 'tags_name_th'] = way_tags.get("name:th", None)
                        df.loc[original_df_index, 'tags_name_en'] = way_tags.get("name:en", None)
                        df.loc[original_df_index, 'speedlimit'] = result.get("speed_limit", None)

                        coord = (df.loc[original_df_index, 'Lat'], df.loc[original_df_index, 'Lon'])
                        cache[coord] = {
                            'nearest_file': result.get("nearest_file", None),
                            'nearest_node_id': result.get("nearest_node", {}).get("id", None),
                            'way_info_id': result.get("way_id", None),
                            'highway_type': way_tags.get("highway", None),
                            'tags_name_th': way_tags.get("name:th", None),
                            'tags_name_en': way_tags.get("name:en", None),
                            'speedlimit': result.get("speed_limit", None)
                        }

            except aiohttp.ClientError as e:
                print(f"API request failed on {ecs_api_url}: {e}")

    async def main():
        connector = aiohttp.TCPConnector(limit_per_host=100)
        semaphore = asyncio.Semaphore(100)
        timeout = aiohttp.ClientTimeout(total=60*5)  # Increase timeout to 5 minutes
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for i in range(0, len(df), batch_size):
                ecs_api_url = ecs_api_urls[(i // batch_size) % len(ecs_api_urls)]
                tasks.append(process_batch(session, i, min(i + batch_size, len(df)), ecs_api_url, semaphore))
            await asyncio.gather(*tasks)

    asyncio.run(main())

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=f'mid_{file_key}', Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data processed successfully'})
    }

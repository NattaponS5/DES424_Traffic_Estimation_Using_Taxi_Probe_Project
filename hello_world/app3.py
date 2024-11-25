import pandas as pd
import os
import boto3
import io

s3 = boto3.client('s3')

def process_chunk(chunk):
    df_filtered = chunk[chunk['EngineAcc'] == 1]

    def keep_middle_85(group):
        if len(group) <= 10:
            return group
        sorted_group = group.sort_values(by='Speed')
        lower_bound = int(len(sorted_group) * 0.075)
        upper_bound = int(len(sorted_group) * 0.925)
        return sorted_group.iloc[lower_bound:upper_bound]

    df_filtered = df_filtered.groupby('way_info_id').apply(keep_middle_85).reset_index(drop=True)
    return df_filtered

def assign_weight(row):
    return 0.2 if row['ForHireLight'] == 1 else 0.8

def get_avg_speed(data_frame):
    data_frame['weight'] = data_frame.apply(assign_weight, axis=1)
    data_frame['weighted_speed'] = data_frame['Speed'] * data_frame['weight']
    df_avg_speed = data_frame.groupby('way_info_id').apply(
        lambda group: pd.Series({
            'average_speed': group['weighted_speed'].sum() // group['weight'].sum(),
            'tags_name_en': group['tags_name_en'].dropna().iloc[0] if not group['tags_name_en'].dropna().empty else None,
            'speedlimit': group['speedlimit'].iloc[0]
        })
    ).reset_index()
    return df_avg_speed

def determine_color(row):
    speedlimit = row['speedlimit']
    avg_speed = row['average_speed']

    if speedlimit is not None:
        if avg_speed > 0.6 * speedlimit:
            return 'green'
        elif avg_speed > 0.3 * speedlimit:
            return 'orange'
        else:
            return 'red'
    else:
        if avg_speed > 40:
            return 'green'
        elif avg_speed > 21:
            return 'orange'
        else:
            return 'red'

def main_call(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    chunks = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf-8', chunksize=10000)
    
    processed_chunks = []
    for chunk in chunks:
        chunk.dropna(subset=['way_info_id', 'Speed', 'EngineAcc'], inplace=True)
        chunk['way_info_id'] = chunk['way_info_id'].astype(str)
        chunk = chunk[chunk['EngineAcc'] == 1]
        chunk = chunk[chunk['highway_type'].notnull()]
        processed_chunk = process_chunk(chunk)
        processed_chunks.append(processed_chunk)
    
    df_filtered = pd.concat(processed_chunks)
    df_avg_speed = get_avg_speed(df_filtered)
    df_avg_speed['color'] = df_avg_speed.apply(determine_color, axis=1)

    base_name = os.path.splitext(os.path.basename(key))[0]
    output_filename = f"final_{base_name[4:]}.csv"
    csv_buffer = io.StringIO()
    df_avg_speed.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=output_filename, Body=csv_buffer.getvalue())
    return f"Results saved to {output_filename}"

def lambda_handler():
    bucket = 'taxi-20180801-5min'
    key = 'mid_taxi_20180801_0900.csv'
    result = main_call(bucket, key)
    return {
        'statusCode': 200,
        'body': result
    }
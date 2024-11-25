import requests
import json

def lambda_handler():  # Include event and context

    url = "http://15.152.74.164:5000/find_nearest_node"
    headers = {'Content-Type': 'application/json'}

    # Example payload (replace with your actual data)
    payload = {
        "points": [
                {"point_lat": 13.65305, "point_lon": 13.65305},
                {"point_lat": 13.7336, "point_lon": 100.55304},
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload)  # Use requests.post()
        response.raise_for_status()  # Check for other errors (4xx or 5xx)
        print("Request successful:")
        data = response.json()  # Parse JSON response
        print(json.dumps(data, indent=4)) # Print nicely formatted
    except requests.exceptions.RequestException as e:

        print(f"API request failed: {e}")
        return {  # Return an error from Lambda if the API call failed.
            'statusCode': 500,  # Indicate error
            'body': json.dumps({'error': str(e)})  # Include details
        }


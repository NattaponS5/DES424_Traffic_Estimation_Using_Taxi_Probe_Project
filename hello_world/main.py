import json
from app7 import lambda_handler

# Define a sample event and context
event = {}
context = {}

# Call the lambda_handler function
response = lambda_handler()
# Print the response
print(json.dumps(response, indent=4))
# Lambda Function Workflow Documentation

This document outlines the workflow of the Lambda functions used to run various scripts that operate different functionalities. Each function is designed to perform a specific task, and they are organized sequentially to achieve the desired outcome.

## Table of Contents
1. [Fetch Road Information](#fetch-road-information)
2. [Conclude Information](#conclude-information)
3. [Add Data to DynamoDB](#add-data-to-dynamodb)
4. [Query Data in DynamoDB](#query-data-in-dynamodb)
5. [Show Map](#show-map)
6. [Save HTML to S3 and Host](#save-html-to-s3-and-host)
7. [Configuration in main.py](#configuration-in-mainpy)

---

## Fetch Road Information

**Script**: `hello_world/app.py`  
**Entry Point**: `main.py`  

### Description
This script runs an ECS task to fetch road information from a specified source. The fetched data is then saved to a file named `mid_{basename}`.

### Workflow
1. **ECS Task Execution**: The script triggers an ECS task to fetch road information.
2. **Data Saving**: The fetched road information is saved to `mid_{basename}`.

---

## Conclude Information

**Script**: `hello_world/app3.py`  
**Entry Point**: `main.py`  

### Description
This script reads the data saved in `mid_{basename}` and processes it to conclude relevant information. The processed data is then saved to a file named `final_{basename}`.

### Workflow
1. **Data Reading**: The script reads the data from `mid_{basename}`.
2. **Information Concluding**: The script processes the data to extract relevant information.
3. **Data Saving**: The concluded information is saved to `final_{basename}`.

---

## Add Data to DynamoDB

**Script**: `hello_world/app4.py`  
**Entry Point**: `main.py`  

### Description
This script reads the data saved in `final_{basename}` and adds it to a DynamoDB table.

### Workflow
1. **Data Reading**: The script reads the data from `final_{basename}`.
2. **DynamoDB Insertion**: The script inserts the data into a DynamoDB table.

---

## Query Data in DynamoDB

**Script**: `hello_world/app5.py`  
**Entry Point**: None (Direct Execution)  

### Description
This script queries data from a DynamoDB table and processes the results.

### Workflow
1. **DynamoDB Query**: The script queries the DynamoDB table.
2. **Data Processing**: The script processes the queried data.

---

## Show Map

**Script**: `hello_world/app6.py`  
**Entry Point**: None (Direct Execution)  

### Description
This script pulls data from a DynamoDB table and uses it to generate a map visualization.

### Workflow
1. **Data Pulling**: The script pulls data from the DynamoDB table.
2. **Map Generation**: The script generates a map visualization using the pulled data.

---

## Save HTML to S3 and Host

**Script**: `hello_world/app7.py`  
**Entry Point**: None (Direct Execution)  

### Description
This script saves an HTML file to an S3 bucket, generates a presigned URL for the file, and hosts it using AWS Amplify.

### Workflow
1. **HTML Saving**: The script saves the HTML file to an S3 bucket.
2. **Presigned URL Generation**: The script generates a presigned URL for the saved HTML file.
3. **Hosting**: The script hosts the HTML file using AWS Amplify.

---

## Configuration in `main.py`

The `main.py` script is configured to dynamically import and execute the appropriate Lambda handler based on the value of `i`. 

### Configuration
```python
# main.py
from app{i} import lambda_handler

This is the lambda function that used to run the script the operate functions.

python hello_world/app.py --- python main.py

run ecs to fetch road information and save to mid_{basename}

--------------------------------------------------

python hello_world/app3.py --- python main.py

read mid_{basename} conclude infor and save to final_{basename}

--------------------------------------------------

python hello_world/app4.py --- python main.py

add final data to dynamodb

--------------------------------------------------

python hello_world/app5.py ---- no need main.py

query data in dynamodb

--------------------------------------------------

python hello_world/app6.py

show map by pull from dynamodb

--------------------------------------------------

python hello_world/app7.py

save html to s3 + get presigned URL + host to Amplify

________________________________________________
|                                              |
|   please change i value ---- in main.py      |   
|   from app{i} import lambda_handler          |
|                                              |
------------------------------------------------
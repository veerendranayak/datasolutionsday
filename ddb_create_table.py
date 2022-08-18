from logging import exception
import boto3
import time

dynamodb = boto3.client('dynamodb')
tableidentifier='questmember'
gsiidentifier='gsi1'
try:
    dynamodb.delete_table(TableName=tableidentifier)
except Exception:
    pass

try:
    time.sleep(30)
    dynamodb.create_table(
        TableName=tableidentifier,
        AttributeDefinitions=[
            {
                "AttributeName": "PK",
                "AttributeType": "S"
            },
            {
                "AttributeName": "SK",
                "AttributeType": "S"
            }
        ],
        KeySchema=[
            {
                "AttributeName": "PK",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "SK",
                "KeyType": "RANGE"
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Table created successfully.")
    time.sleep(30)
    dynamodb.update_table(
        TableName=tableidentifier,
        AttributeDefinitions=[
            {
                "AttributeName": "PK",
                "AttributeType": "S"
            },
            {
                "AttributeName": "SK",
                "AttributeType": "S"
            }
        ],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": gsiidentifier,
                    "KeySchema": [
                        {
                            "AttributeName": "SK",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "PK",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    }
                }
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Table updated successfully.")
except Exception as e:
    print("Could not create table. Error:")
    print(e)

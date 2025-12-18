import boto3
from moto import mock_aws 

import json_schema_to_hive as js_2_hive

@mock_aws  
def main():
    _S3_CLIENT = boto3.client("s3", region_name='us-east-1')
    _S3_CLIENT.create_bucket(Bucket='iti-query-results')

    _ATHENA_CLIENT = boto3.client('athena', region_name='us-east-1')

    js_2_hive._ATHENA_CLIENT = _ATHENA_CLIENT

    response = js_2_hive.handler('events', 's3://iti-query-results/')

    print(f"Response: {response}")
    
if __name__ == "__main__":
    main()
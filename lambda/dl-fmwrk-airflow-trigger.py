import json
import boto3
import http.client
import base64

mwaa_env_name = 'dl-fmwrk-mwaa-env'
mwaa_cli_command = 'dags trigger '

client = boto3.client('mwaa')

def lambda_handler(event, context):
    object_key = event["Records"][0]["s3"]["object"]["key"]
    dag_name = object_key.split("/")[1] + "_" + object_key.split("/")[2] + "_workflow"
    
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    
    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    payload = mwaa_cli_command + dag_name
    headers = {
      'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
      'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    
    print(f"Airflow dag {dag_name} has been triggered")
    print(dict_str)
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Airflow dag {dag_name} has been triggered")
    }

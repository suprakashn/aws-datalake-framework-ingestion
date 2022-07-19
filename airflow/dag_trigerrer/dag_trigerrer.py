import logging
import os
import json

import datetime
import requests


def lambda_handler(event, context):
    s3_path = event["Records"][0]["s3"]["object"]["key"]
    s3_path_list = str(s3_path).split("/")
    
    if (len(s3_path_list) == 4 and len(s3_path_list[3]) > 0):
        src_sys_id = s3_path_list[1]
        asset_id = s3_path_list[2]
        DAG_ID = f"{src_sys_id}_{asset_id}_workflow"
        print(DAG_ID)
        url = f"https://www.dl-fmwrk-airflow-domain.click/api/v1/dags/{DAG_ID}/dagRuns"
            
        payload = "{}"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic YWRtaW46YWRtaW4=",
            'cache-control': "no-cache",
            'postman-token': "6a13f23f-eec2-c51f-74d7-c0ebd612545a"
            }
        
        response = requests.request("POST", url, data=payload, headers=headers)
        
        print(response.text)

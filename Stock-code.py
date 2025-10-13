import requests
from io import StringIO
import json
import pandas as pd
pd.set_option('display.max_columns', None)
import csv
import boto3
import consonants as constant
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# github_api_url = "https://api.github.com/repos/squareshift/stock_analysis/contents/"

#Calling ssm and sns
client = boto3.client("ssm")
sns = boto3.client("sns", region_name="eu-north-1")
ssm = boto3.client("ssm", region_name="eu-north-1")
s3 = boto3.client('s3')
########
def send_sns_success():
    try:
        success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        print("SARN-Rasagna",success_sns_arn)
        component_name = constant.COMPONENT_NAME
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        success_msg = constant.SUCCESS_MSG
        sns_message = f"{component_name} : {success_msg}"
        logger.info(f"Sending SNS Success Message: {sns_message}")
        succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return succ_response
    except Exception as sns_error:
        logger.error("Failed to send success SNS", exc_info=True) 
def send_error_sns():
    try:
        error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        error_message = constant.ERROR_MSG
        component_name = constant.COMPONENT_NAME
        sns_message = f"{component_name} : {error_message}"
        logger.error(f"Sending SNS Error Message: {sns_message}")
        err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return err_response
    except Exception as sns_error:
        logger.error("Failed to send error SNS", exc_info=True)
# print(response)
def save_to_s3(df, bucket_name, s3_path):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=True, index=False)
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())
    return f"s3://{bucket_name}/{s3_path}"
try:
    url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
    response = requests.get(url)
    files = response.json()
    s3 = boto3.client('s3')
    # print(files)
    csv_files = [file['download_url'] for file in files if file['name'].endswith('.csv')]
    a=csv_files[0]
    file_name = a.split("/")[-1].replace(".csv", "")
    # print(a)
    # print(file_name)
    # print(csv_files)
    csv_file = csv_files.pop()
    # print(csv_file)
    d = pd.read_csv(csv_file)
    # print(d)
    # print(d.columns)
    # print(d["Sector"])
    dataframes=[]
    file_names=[]
    for url in csv_files:
        file_name = url.split("/")[-1].replace(".csv", "")
        df = pd.read_csv(url)
        df['Symbol'] = file_name
        dataframes.append(df)
        file_names.append(file_name)
    # print(dataframes)
    # print(file_names)
    combined_df = pd.concat(dataframes, ignore_index=True)
    # print(combined_df)
    # print(combined_df.columns)
    o_df = pd.merge(combined_df,d,on='Symbol',how='left')
    # print(o_df)
    # print(o_df.columns)
    # print(o_df[2:5])
    result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
    # print(result)
    # print(o_df["timestamp"])
    o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
    # print(o_df["timestamp"])
    filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <= "2021-05-26")]
    # print(filtered_df)
    result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
    list_sector = ["TECHNOLOGY","FINANCE"]
    df = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
except Exception as e:
    print(f"Lambda function error: {str(e)}")
    send_error_sns()
    print("Error mail has been sent")


def lambda_handler(event, context):
    """AWS Lambda handler function."""
    try:       
        bucket_name = 'rasagna-1090'
        s3_path = 'output/result.csv'

        save_to_s3(df, bucket_name, s3_path)
        send_sns_success()
        print("Success mail has been sent")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Data saved to {bucket_name}/{s3_path}')
        }

    except Exception as e:
        print(f"Lambda function error: {str(e)}")
        send_error_sns()
        print("Error mail has been sent")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
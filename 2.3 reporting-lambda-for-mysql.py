# Reporting Lambda for MySQL is deployed in a Container.
# Other files necessary for docker container building,
# please refer to the repo of 'DockerImage'

'''
Parent Lambda:
1) to validate s3 event, s3 key 
2) to check the file and split the file if it exceeds the limit
3) to decide to invoke ECS container for Database Init or to invoke child lambda for Data ETL/Analysis
4) ----waiting for child response -------
5) to backup the data file in another s3 bucket 
6) to invoke loading lambda for report data creation
7) ----waiting for child response -------
8) to invoke reporting lambda for data export
9) ----waiting for child response -------
10) to send email and notify person in charge that the Data processing is successful or not
11) return failure message to SQS if any

Loading Lambda:
1) to get task from parent 
2) to connect Database and load data using Procedures (predefined by ECS task)
  a) to create temp table in MySQL
  b) to load data from data file to temp table
  c) to check if all data can be loaded to temp table
  d) if yes, to load data from temp table to permanent table in MySQL 
3) to respond to leader lambda for data ETL
4) to get task from parent for report data
5) to execute stored procedures in MySQL for report data creation
6) to respond to leader lambda for report data


Reporting Lambda:
1) to get task from parent
2) to export data from MySQL to s3 using MySQL command line
3) to save the report data in the .csv file
4) to respond to leader lambda for report data
'''

import subprocess
import os
import time

import json
import boto3
import botocore
s3_client=boto3.client('s3')
s3_resource = boto3.resource('s3')

# get environment variables 
#=os.environ['']
aws_region=os.environ['aws_region']
mysql_database = os.environ['mysql_database']
mysql_host=os.environ['mysql_host']
mysql_host_name=mysql_host[0:-5]
backup_bucket=os.environ['backup_bucket']
timestamp = time.strftime('%Y-%m-%d-%I:%M')
print('current time is : {}'.format(timestamp))

topic_arn_on_failure=os.environ['topic_arn_on_failure']
topic_arn_on_success=os.environ['topic_arn_on_success']
topic_name_on_failure=os.environ['topic_name_on_failure']
topic_name_on_success=os.environ['topic_name_on_success']

# get username/password from secrets manager
secret_name=os.environ['secret_name']
session=boto3.session.Session()
session_client=session.client(
    service_name='secretsmanager',
    region_name=aws_region
)    
secret_response=session_client.get_secret_value(
    SecretId=secret_name
)        
secret_arn=secret_response['ARN']
secretstring=secret_response['SecretString']
secret_json=json.loads(secretstring)
user_name=secret_json['username']
pass_word=secret_json['password']

def handler(event, context):
    #get variables from parent lambda:
    report_source_data=event['report_source_data']
    report_source_data_folder=event['report_source_data_folder']
    reporting_status=0
    try:
        # after the reporting lambda is invoked by loading or leader lambda
        # reporting lambda will export data from rds mysql to s3 using CLI
        # the reason to do so is that mysql can't export data to s3 directly
        # there is no other way but to use command line to finish this task
        #command = "mysqldump -h %s -u %s -p%s %s %s | gzip -c | aws s3 cp - s3://%s/%s/%s.gz" % (
            #mysql_host_name, user_name, pass_word, mysql_database, report_source_data,backup_bucket, backup_folder,report_source_data + "_" + timestamp)
        
        # belows can only get metadata of a mysqltable 
        #command = "mysqldump -h %s -u %s -p%s %s %s --single-transaction --quick --no-tablespaces | gzip | aws s3 cp - s3://%s/%s/%s.sql.gz" % (
            #mysql_host_name, user_name, pass_word, mysql_database, report_source_data,backup_bucket, report_source_data_folder,report_source_data + "_" + timestamp)
        
        #command = "mysqldump -h %s -u %s -p%s --no-create-info %s %s --single-transaction --quick --no-tablespaces | gzip | aws s3 cp - s3://%s/%s/%s.csv.gz" % (
            #mysql_host_name, user_name, pass_word, mysql_database, report_source_data,backup_bucket, report_source_data_folder,report_source_data + "_" + timestamp)
        
        #save_as = "{}_{}.sql.gz".format(report_source_data,timestamp)
        #command="mysqldump -h %s -u %s -p%s --no-create-info %s %s --single-transaction --quick --no-tablespaces | gzip > /tmp/%s " % (
            #mysql_host_name,user_name,pass_word,mysql_database,report_source_data,save_as
        #)
        #subprocess.Popen(command, shell=True).wait()

        #command="aws s3 cp /tmp/{} s3://{}/{}/{}".format(save_as,backup_bucket,report_source_data_folder,save_as)
        #subprocess.Popen(command, shell=True).wait()

        #command="rm /tmp/{}".format(save_as)
        #subprocess.Popen(command, shell=True).wait()

        # below method can get data instead of metadata of MySQL table
        save_as = "{}_{}.csv".format(report_source_data,timestamp)
        command='mysql -h %s -u %s -p%s -D %s --batch --quick -e "select * from %s" > /tmp/%s' % (
            mysql_host_name,user_name,pass_word,mysql_database,report_source_data,save_as
        )
        subprocess.Popen(command, shell=True).wait()
        # upload the .csv file from /tmp/ to s3
        command="aws s3 cp /tmp/{} s3://{}/{}/{}".format(save_as,backup_bucket,report_source_data_folder,save_as)
        subprocess.Popen(command, shell=True).wait()
        # clear the lambda tmp folder
        command="rm /tmp/{}".format(save_as)
        subprocess.Popen(command, shell=True).wait()

        print("MySQL backup finished")

    except Exception as e:
        print_content='error when exporting data from MySQL to S3, description: {}'.format(e) 
        print(print_content)
        reporting_status=0
        return {
            "reporting_status":reporting_status,
            "error":print_content
        }
        
    print('Exporting from MySQL to S3 completed!')
    reporting_status=1
    return {
        "reporting_status":reporting_status,
        "error":''
    }
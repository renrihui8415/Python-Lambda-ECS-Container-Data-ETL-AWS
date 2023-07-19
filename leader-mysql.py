#### Below is to use lambda function (in Python) 
#### to check and initiate MySQL Database, ETL and Analyse Data

###### something we should know about Lambda #######
'''
Lambda can stay in or out of VPC.
If we put it in a VPC, the communication between lambda and other AWS resources (like databases)
in the same VPC would become much easier, but makes communication with resources out of the VPC much harder.
If lambda is out of a VPC, which is a usual practice, as I believe, 
will make lambda super powerful, it can reach out at any resources in your cloud and become a real
leader in your project easily. By way of boto3, nearly every resource can be created and run by lambda.
Using Lambda can achieve a full automation.
In this project, although I wish lambda to be free of VPC, but RDS (MySQL) cannot provided a DATA API
, therefore, lambda can't access it from outside. If we really need leading lambda :
## To set up a parent lambda outside of VPC, and connect and lead the child lambda within MySQL's VPC. 

#======================================
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
from __future__ import print_function
import json
import urllib
import time
from datetime import datetime
from datetime import date
import pytz
from pytz import timezone
import urllib.parse
from urllib.parse import unquote_plus
import unicodedata

### clients settings ###
import boto3
import botocore.session as bc
lambda_client=boto3.client('lambda')
ecs_client=boto3.client('ecs')
s3_client=boto3.client('s3')
s3_resource=boto3.resource('s3')
session=boto3.Session()
s3_session_resource=session.resource('s3')
rds_data_client = boto3.client('rds-data') # this is for aurora only... 

import os
aws_region=os.getenv('aws_region')
Account_Id=os.getenv('Account_Id')
topic_name_on_success=os.getenv('topic_name_on_success')
topic_arn_on_success=os.getenv('topic_arn_on_success')
topic_name_on_failure=os.getenv('topic_name_on_failure')
topic_arn_on_failure=os.getenv('topic_arn_on_failure')
loading_arn=os.getenv('loading_arn')
init_arn=os.getenv('init_arn')

ecs_task_arn=os.getenv('ecs_task_arn')
ecs_cluster_arn=os.getenv('ecs_cluster_arn')
ecs_service_subnets   = os.getenv('ecs_service_subnets')
ecs_service_subnets   =ecs_service_subnets.split(',')
# subnets and groups are string values 
# needs to convert string to list
ecs_security_groups    = os.getenv('ecs_security_groups')
ecs_security_groups    =ecs_security_groups.split(',')
ecs_container_name = os.getenv('ecs_container_name')

#below for backup folder 
backup_bucket=os.getenv('backup_bucket')
backup_folder="backup"
### for read/split data files in s3 ###
import pandas 
from io import StringIO
import math

### time_zone settings ###
time_zone=timezone('EST')
time_zone1="America/New_York"
#print("now is {}".format(datetime.now(time_zone).date()))
today = datetime.now(time_zone).date()
#this can make sure we use est timezone

### timeout settings ###
time_interval_in_minutes=30 #  to load the file if it is uploaded within 30 minutes
lambda_timeout_in_seconds=60
maxerrors_allowed=5

def lambda_handler(event, context):
    stage =0
    file_split=0 # not to split the file, unless it exceeds 6mb
    small_files_folder=''
    schema_name=''
    temp_tablename=''
    tablename=''
    loading_status=0
    if event: 
        #print(event)
        message=event['Records'][0]
        messages_to_reprocess =[]
        # this is a long story, lambda is concurrent and it can't solve the problem of dead lock itself
        # this is why we apply SQS to help reprocess those files who are considered to be dead lock 
        # We set batchsize for the queue to be more than 1, for example, 5. if lambda reports to SQS
        # that only one of the five messages failed to process, the other four files are successfully consumed
        # SQS will treat the whole queue to be a failure. after the invisibility timeout, these five messages 
        # will be reput to the queue again....
        # Fortunately, mapping event has a parameter called 'partial batch response'. it enables lambda to only report 
        # with failed messages to SQS. and SQS only reprocess these failed ones.
        # But according to my experience, setting up this parameter is far not enough. We need to explicitly tell lambda in codes, 
        # which message failed and should be returned to SQS. 
        # The above message_to_reprocess is to collect any failure message.
        batch_failure_response={}
        for record in event["Records"]:
            try:   
                body = record["body"]
                #body=event['Records'][0]['body']
                body=json.loads(body)
    
                s3_event=body['Records'][0]
                event_name=s3_event['eventName']
                print("event_name is {}".format(event_name))
                if "ObjectCreated:" not in event_name:
                    print('event is not s3:create')
                    s3_bucket=s3_event['s3']['bucket']['name']
                    #print("s3_bucket is : {}".format(s3_bucket)) 
                    #=================================
                    delete_message(message)
                    #=================================
                    exit()
                    #do nothing as this is not a valid event
                else:                   
                    s3_bucket=s3_event['s3']['bucket']['name']
                    #print("s3_bucket is : {}".format(s3_bucket)) 
                    s3_key=s3_event['s3']['object']['key']
                    s3_key=unquote_plus(s3_key)
                    #to eliminate the '+' sign in the folder name
                    # this is essential if we try to search for any keys in s3
                    # but in cloudfront client, we need that plus sign
                    print("s3_key is : {}".format(s3_key)) 
     
                    s3_key_size=s3_event['s3']['object']['size']
                    file_size_in_mib=s3_key_size/(1024*1024)
                    #get the data file size
                    print(f"file_size is {s3_key_size/(1024*1024)} MB")
                    #============================================================
                    #get the pure file with/without extension 
                    #get the pure file with lower cases only
                    file_name=os.path.basename(s3_key)
                    length=len(file_name)
                    s3_key_extension=file_name[length-3:]
                    s3_key_extension=s3_key_extension.lower()
                    s3_key_withoutextension=file_name[:length-4]
                    s3_key_withoutextension=s3_key_withoutextension.lower()
                            
                    from_path = "s3://{}/{}".format(s3_bucket, s3_key)

                    #create file path for data file in s3
                    #print("URI of s3_key is {}".format(from_path))
                    
                    if 'backup' in s3_bucket and s3_key_extension == "sql":
                        print_content='File to init DB has been uploaded to {}. Start running ECS task'.format(from_path)
                        print(print_content)
                        file_delimiter=';'
                        #means sql files for db init uploaded to backup bucket 
                        response=ecs_client.run_task(
                            taskDefinition=ecs_task_arn,
                            launchType='FARGATE',
                            cluster=ecs_cluster_arn,
                            platformVersion='LATEST',
                            count=1,
                            networkConfiguration={
                                'awsvpcConfiguration': {
                                    'subnets': ecs_service_subnets,
                                    #'assignPublicIp': 'DISABLED',
                                    # for testing , use public ip
                                    # for prod, disable public ip
                                    'assignPublicIp': 'ENABLED',
                                    'securityGroups': ecs_security_groups
                                }
                            },
                            overrides={
                                'containerOverrides': [
                                    {   
                                        'name': ecs_container_name,
                                        'command':["python", "/rds_init.py"],
                                        'environment': [
                                            {
                                                'name': 'task',
                                                'value': 'rds_init'
                                            },
                                            {
                                                'name': 'file_name',
                                                'value': file_name
                                            },
                                            {
                                                'name': 's3_bucket',
                                                'value': s3_bucket
                                            },
                                            {
                                                'name': 's3_key',
                                                'value': s3_key
                                            },
                                            {
                                                'name': 's3_key_withoutextension',
                                                'value': s3_key_withoutextension
                                            }                      
                                        ]
                                    }
                                ]
                            }
                        )
                        print("Trying to get response from ecs container : {}".format(response))
                        '''
                        # below method can't check ECS task running status
                        # the response always show failure is 'missing'
                        # AWS only has a way to check the task by exporting ecs logs to cloudwatch
                        # this has been done in container definition
                        print("getting response from ecs container : {}".format(response))
                        # this response will just let you know if a ecs task is initiated or not
                        for task in response['tasks']:
                            attachments=task['attachments']
                            #attachments=json.load(attachments)
                            
                            task_id=attachments[0]['id']
                        print('task_id is : {}'.format(task_id))
                        # to check if a task is successfully run in the end or not
                        attempts=0
                        MAX_WAIT_CYCLES=3
                        print('checking container running status')
                        while attempts<MAX_WAIT_CYCLES:
                            attempts +=1
                            time.sleep(3)
                            resp = ecs_client.describe_tasks(
                                cluster=ecs_cluster_arn,
                                tasks=[
                                    task_id,
                                ]
                            )
                            print('>>>>>>>>>>>>>')
                            print("GETTING RESPONSE AFTER CHECKING TASK : {}".format(resp))
                            exit_code=resp['tasks'][0]['containers'][0]['exitCode']
                            print("exit_code is {}".format(exit_code))
                            failure_reason=resp['failures'][0]['reason']
                            print("failure_reason is {}".format(failure_reason))
                            failure_detail=resp['failures'][0]['detail']
                            print("failure_detail is {}".format(failure_detail))
                            print('>>>>>>>>>>>>>')
                        '''
                        # since we can't let lambda check the task is successful or not
                        # we need to refer to cloudwatch logs to see if everything goes well
                        exit() 
                             
                    elif 'upload' in s3_bucket and (s3_key_extension == "txt" or s3_key_extension == "csv"):
                            #if the uploaded file is a flat file
                            #print('the file can be processed further')  
                            ### attention:
                            ### if data set is large, use ECS 
                            ### if data set is small, use child lambda
                            if s3_key_extension=='csv':
                                file_delimiter=','
                            elif s3_key_extension=='txt': 
                                file_delimiter=','
                            resp=get_tablename(s3_key_withoutextension) 
                            schema_name=  resp['schema_name'] 
                            temp_tablename=resp['temp_tablename']
                            tablename=resp['correct_tablename']
                            if tablename=='':
                                #=================================
                                delete_message(message)
                                #=================================
                                exit()

                            #print('The file of {} is ready to be loaded to MySQL'.format(s3_key))
                            stage =1
                            #============================================================
                            # if the size is more than 6mb, will split it into smaller ones 
                            if file_size_in_mib>5.6:
                                file_split=1
                                response=split_file(s3_bucket,s3_key,file_delimiter,file_size_in_mib)
                                small_files_folder=response['small_files_folder']

                            stage=2

                            # to invoke loading lambda
                            # print("invoking loading_lambda")
                            
                            inputParams = {
                                "file_split":file_split,
                                "small_files_folder":small_files_folder,
                                "file_name":file_name,
                                "s3_bucket":s3_bucket,
                                "s3_key":s3_key,
                                "file_delimiter":file_delimiter,
                                "schema_name":schema_name,
                                "temp_tablename":temp_tablename,
                                "tablename":tablename
                            }
                            responseFromChild=invoke_another_lambda(inputParams,1,2)  
                
                            loading_status =responseFromChild['loading_status']

                            ## after we get the response from loading function 
                            ## to backup or return failure message
                            if loading_status==1:
                                #means loading succeeded
                                print('loading succeeded!')
                                ##### EEE) move the file into another s3 bucket for backup ######
                                stage=3
                                backup_file(s3_bucket,s3_key,file_name)
                                #=================================
                                delete_message(message)
                                #=================================
                                exit()
                            else:
                                # loading lambda reports failure
                                if loading_status==2:
                                    # the file does not exist according to loading lambda
                                    print_content="There is no such file {}. It might be processed and moved to backup by another lambda.".format(s3_key)
                                    print(print_content)
                                    #=================================
                                    delete_message(message)
                                    #=================================
                                    exit()
                                else:                            
                                    print_content='loading of {} failed according to loading lambda. error: {}. Please check the log table in MySQL'.format(s3_key,responseFromChild['error'])      
                                    print(print_content)
                                    messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                                    print('Adding failed file into reprogress response list')
                    else: # the file is not csv, txt or sql
                        print ('the file of {} is not for DB init or Data ETL.'.format(s3_key))
                        delete_message(message)
                        exit()
            except Exception as e:
                if stage==0 : 
                    print_content='stage={}, error when checking files in s3, description: {}'.format(stage,e)
                if stage ==1:
                    print_content='stage={}, error when splitting large files, description: {}'.format(stage,e)
                if stage ==2:
                    print_content='stage={}, error when invoking loading lambda, description: {}'.format(stage,e) 
                if stage ==3:
                    print_content='stage={}, error when backing up files, description: {}'.format(stage,e)
                #means error not caused by loading,
                #the file needs to be reprocessed
                print(print_content)
                messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                print('Adding failed file into reprogress response list')

                subject=print_content
                body=e
                #notify(0,subject,body)
              
        batch_failure_response["batchItemFailures"] = messages_to_reprocess
        if len(messages_to_reprocess) ==0:
            #means the list is empty
            print('message consumed successfully.')
            return batch_failure_response
        else: #means there's message in the list
            list_to_string=' '.join(map(str, messages_to_reprocess))
            print('There''s error message :{}'.format(list_to_string))
            print('returning failed file to sqs...')
            return batch_failure_response

def invoke_another_lambda(inputParams,invocationType,child_lambda=2):
    # 1 --> other child lambda , for example
    # 2 --> loading lambda (by default)

    # 1 --> get response
    # 2 --> async (parent lambda dont need to wait for child lambda's response )
    if child_lambda ==1: 
        function_arn=init_arn
        print("invoking child lambda")
    else:
        function_arn=loading_arn
        print("invoking loading lambda") 

    if invocationType ==1:
        type='RequestResponse'
    else:
        type='Event'
    '''
    # the project decided to use VPC ENDPOINT for security reason
    # loading lambda in the VPC will fetch secret itself
    # leader lambda in the public is not allowed to connect Secrets Manager
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

    inputParams['secret_json']=secret_json
    # added RDS credentials and passed it to child lambda
    '''
    response = lambda_client.invoke(
        FunctionName = function_arn,
        InvocationType = type,
        Payload = json.dumps(inputParams)
    )
    responseFromChild = json.load(response['Payload'])
    return responseFromChild
#==========================================================================
def split_file(source_bucket_name, source_s3_key,file_delimiter,file_size_in_mib,target_bucket_name=backup_bucket,target_foler=backup_folder):   
    
    resp = s3_client.get_object(Bucket=source_bucket_name, Key=source_s3_key)
    data = pandas.read_csv(resp['Body'],sep=file_delimiter)
 
    # to drop the first row of headers
    data.index=data.index+1
    row_number=len(data.index)
    # to make index starting from 1
    file_name=os.path.basename(source_s3_key)
    length=len(file_name)
    s3_key_extension=file_name[length-3:]

    # below is to split the large data files into smaller one
    # Please remember there is no way to save any files in the upload s3 bucket
    # as s3 bucket will trigger sqs that will trigger lambda
    # this is an horrible infinite loop
    # the split files are all saved in another bucket
    small_files_folder=finding_folder(target_bucket_name,target_foler,'{}/{}'.format(today,file_name))

    csv_buffer=StringIO
    lines_per_file=int(row_number*(5/file_size_in_mib))
    ranges_number=math.ceil(row_number/lines_per_file)
    print('total {} small files'.format(ranges_number) )

    if row_number % lines_per_file > 0:
        # means the last range is not the same large as the previous ranges
        # which is normal
        last_range_number=row_number % lines_per_file
        # get the correct row number for the last range
    else:
        last_range_number=0
    print ('the last range has {} rows'.format(last_range_number) )

    def range1(start, end):
        return range(start,end+1)

    for splitnumber in range1(1,ranges_number):
        print (splitnumber)
        small_files_name=file_name.replace('.{}'.format(s3_key_extension),'_{}.{}'.format(s3_key_extension))
        small_file_data=data.loc[((splitnumber-1)*lines_per_file+1):(splitnumber*lines_per_file)]
        small_file_data.to_csv(csv_buffer,index=False)
        # to save the data into backup folder
        save_as='{}{}'.format(small_files_folder,small_files_name)
        s3_session_resource.Object(target_bucket_name,save_as).put(Body=csv_buffer.getvalue())
        csv_buffer.truncate(0) 
        # remember to truncate the buffer, as the buffer is still in loop
        # it can't truncate itself for the next split part of the data
    return {
        "small_files_folder":small_files_folder
    }
#==========================================================================
def finding_folder(bucket_name,folder_name,subfolder_name):
    folder_combination='{}/{}/'.format(folder_name,subfolder_name)
    folder_found=0
    #folder_found=0 means neither folder nor subfolder found
    #folder_found=1 means only folder found, need to create a subfolder
    #folder_found=2 means both the folder and subfolder found
    
    target_folder=None
    #no path for target folder and subfolder yet
    
    objects_in_bucket=s3_client.list_objects_v2(Bucket=bucket_name)
    #as the bucket is empty at the first place, there wont be any content
    if 'Contents' in objects_in_bucket:
        for object in objects_in_bucket['Contents']:
            if folder_name in object['Key']:
                #folder found
                substring=object['Key']
                if substring.endswith('{}/'.format(folder_name)):
                    folder_alone=object['Key']
                    folder_path='{}{}/'.format(folder_alone,subfolder_name)
                    folder_found=1
                    #here to grab the backup folder without its subfolders
                    
                elif substring.endswith('{}/'.format(subfolder_name)):
                    print('subfolder found:'.format(object['Key']))
                    folder_found=2
                    target_folder=object['Key']
                    
                    return target_folder
    else:
        folder_found=0
        
    if folder_found==1:
        #means folder found alone, missing today's subfolder
        s3_client.put_object(Bucket=bucket_name, Key=folder_path)
        print("today's subfolder created:{}".format(folder_path))
        target_folder=folder_path
    if folder_found==0:
        s3_client.put_object(Bucket=bucket_name, Key=folder_combination)
        print('Both of folder and subfolder created.'.format(folder_combination))
        target_folder=folder_combination
    return target_folder
    # the result contains '/' in the end
#==========================================================================                    
def get_tablename(target_name):
    schema_name=''
    correct_tablename=''
    temp_tablename=''
    #we can add spelling corrector here if there's possibility of typos in filenames 
    if 'price' in target_name or 'index' in target_name:
        #always be aware of lower cases
        #make names to be in lower cases as soon as possible
        #s3_key_withoutextension already be lower case
      
        schema_name='here is your mysql schema name' 
        correct_tablename='0.PriceIndex'        
        # to name the file's temp table in mysql:
        temp_tablename='{}_temp'.format(target_name)
        
    #elif 'some_string' in target_name:
        #more schemas and tables here

    else:
        #print ('The file name of {} is not valid. Please check if you uploaded the right file.'.format(s3_key))
        #print('Sending email of notification via SNS....')
        subject='The file name of {} is not valid. Please check if you uploaded the right file.'.format(target_name)
        body=''
        res = notify(0, subject, body)
        exit()
    # if no table matches, this file won't be loaded 
    return {
        "schema_name":schema_name,
        "temp_tablename":temp_tablename,
        "correct_tablename":correct_tablename
    }
#===============================================================================
def notify(status, subject, body):
    #here status decides which topic to call:
    if status==1:
        #means everythin going well
       sns_topic_arn=topic_arn_on_success
    elif status==0:
        sns_topic_arn=topic_arn_on_failure
    #print("topic_arn is : {}".format(sns_topic_arn))
    subject = ("".join(ch for ch in subject if unicodedata.category(ch)[0] != "C"))[0:99]
    body = str(body)
    sns_client = boto3.client('sns')
    response = sns_client.publish(
        TargetArn=sns_topic_arn,
        Subject=subject,
        Message=body
        #if we wish to send different messages to mobile and email
        # we can set as below:
        #Message = json.dumps({'default': subject}),
        #MessageStructure='json'
    )
    return "message sent"
#==========================================================================
def backup_file(source_bucket_name,source_s3_key,backup_filename,backup_bucketname=backup_bucket,backup_foldername=backup_folder):
    #define the source
    print('starting back-up.')
    copy_source={
        'Bucket':source_bucket_name,
        'Key':source_s3_key
    }
    #define the target
    #to find the backup folder and today's subfolder,
    #if not existing, create new ones
    respon=finding_folder(backup_bucketname,backup_foldername,today)
    #target_folder=respon
    #that is the folder, the data file will be moved into.

    #to create target file     
    backup_file='{}{}'.format(respon,backup_filename)
    s3_client.put_object(Bucket=backup_bucketname, Key=backup_file)
    #to move the data file to the target position for back up
    bucket=s3_resource.Bucket(backup_bucketname)
    bucket.copy(copy_source,backup_file)
    s3_resource.Object(source_bucket_name,source_s3_key).delete()
    print('file of {} has been moved to backup folder of {}'.format(source_s3_key,respon))
    
#=====================================================================================                            
#to delete sqs message after a successful loading
def delete_message(message):
    #print("message for deletion: {}".format(message))
    sqs_region, sqs_Account_Id, sqs_name = message['eventSourceARN'].split(':')[3:6]
    sqs_client = boto3.client('sqs', region_name=sqs_region)
    sqs_url = f'https://{sqs_region}.queue.amazonaws.com/{sqs_Account_Id}/{sqs_name}'
    #sqs_client.delete_message(QueueUrl=sqs_url, ReceiptHandle=message['receiptHandle'])
    
    #print('sqs message deleted')    
    #print("QueueUrl is {} , and ReceiptHandle is {}".format(sqs_url,message['receiptHandle']))
#==========================================================================     


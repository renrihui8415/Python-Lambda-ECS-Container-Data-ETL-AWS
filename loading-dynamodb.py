#### Below is to use lambda function (in Python) 
#### to create Dynamodb Table and ETL 

#### NOTE #####
# The logs may show below error:
# "Unable to map value based on the given schema. Remainder of the file will not be processed."
# this is caused by uploading data file using AWS S3 console
# The import will be successful in spite of this error.
# Uploading data file using CLI will solve the problem.
#======================================
#below are scripts for loading lambda(parent):
#1 to validate s3 event, s3 key 
#2 to count total rows in the data file
#3 to split the file if it exceeds the limit
#4 to load using 'import table' feature
# there are more than one method to load large dataset into dynamodb
# 'import table feature' allows data in s3 to be loaded into a new dynamodb table
#5 to back up files in s3 if loading completes
#6 to invoke report lambda 
#======================================
#below are scripts for reporting lambda (Child):
#1 to get task from parent 
#2 to export data from dynamodb to s3
##### no analytic query is allowed in dynamodb
##### we have no way but to export raw data to s3
##### and use GLUE and Athena for ETL and reports
##### this is not an ideal reporting process for my project
##### this file is just for reference

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
s3_client=boto3.client('s3')
s3_resource=boto3.resource('s3')
session=boto3.Session()
s3_session_resource=session.resource('s3')
dynamodb_client = boto3.client('dynamodb')

import os
aws_region=os.getenv('aws_region')
Account_Id=os.getenv('Account_Id')
topic_name_on_success=os.getenv('topic_name_on_success')
topic_arn_on_success=os.getenv('topic_arn_on_success')
topic_name_on_failure=os.getenv('topic_name_on_failure')
topic_arn_on_failure=os.getenv('topic_arn_on_failure')

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
                    print("#s3_event is:")
                    print(s3_event)        
                    s3_bucket=s3_event['s3']['bucket']['name']
                    #print("s3_bucket is : {}".format(s3_bucket)) 
                    s3_key=s3_event['s3']['object']['key']
                    s3_key=unquote_plus(s3_key)
                    #to eliminate the '+' sign in the folder name
                    # this is essential if we try to search for any keys in s3
                    # but in some situations like cloudfront client, we need that plus sign
                    print("s3_key is : {}".format(s3_key)) 
     
                    s3_key_size=s3_event['s3']['object']['size']
                    file_size_in_mib=s3_key_size/(1024*1024)
                    #get the data file size
                    print(f"file_size is {s3_key_size/(1024*1024)} MB")
                    #============================================================
                    #get the pure file with/without extension 
                    #get the pure file with lower cases only
                    path_name=os.path.dirname(s3_key)
                    print("path_name is {}".format(path_name))
                    # this path_name will be used later in import_table method
                    file_name=os.path.basename(s3_key)
                    length=len(file_name)
                    s3_key_extension=file_name[length-3:]
                    s3_key_extension=s3_key_extension.lower()
                    s3_key_withoutextension=file_name[:length-4]
                    s3_key_withoutextension=s3_key_withoutextension.lower()
                            
                    from_path = "s3://{}/{}".format(s3_bucket, s3_key)
                    #create file path for data file in s3
                    #print("URI of s3_key is {}".format(from_path))
                             
                    if 'upload' in s3_bucket and (s3_key_extension == "txt" or s3_key_extension == "csv") and ('converted' not in s3_key_withoutextension) :
                        #if the uploaded file is a flat file
                        #print('the file can be processed further')  
                        ### attention:
                        if s3_key_extension=='csv':
                            file_delimiter=','
                        elif s3_key_extension=='txt': 
                            file_delimiter=','
                        # count row_number
                        file=s3_resource.Object(s3_bucket,s3_key)
                        filedata=file.get()['Body'].read()
                        row_number=filedata.decode('ISO-8859-1').count('\n')
                        print('row_number is {}'.format(row_number))

                        resp=get_tablename(s3_key_withoutextension) 
                        tablename=resp['correct_tablename']
                        if tablename=='':
                            #means can't find the target table in dynamodb
                            #=================================
                            delete_message(message)
                            #=================================
                            exit()
                        elif tablename=="error":
                            #means table already exists and can't be deleted for new loading
                            # return message to sqs
                            messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                            print('Adding failed file into reprogress response list')
                        else:
                            #print('The file of {} is ready to be loaded to Dynamodb'.format(s3_key))
                            stage =1
                            #============================================================
                            # if the size is more than 6mb, will split it into smaller ones 
                            if file_size_in_mib>5.6:
                                file_split=1
                                response=split_file(s3_bucket,s3_key,file_delimiter,file_size_in_mib)
                                small_files_folder=response['small_files_folder']

                            stage=2
                            # use 'import_table' feature:
                            ######
                                # as 3 attributes of GEO, Date, Products combines to be PK
                                # in dynamodb, the PK attributes can't exceed 2
                                # To solve the problem, either combine 2 attributes to 1
                                # or add unique id attribute to be pk
                                # this lambda function uses the second solution

                                # before import table, read data file in pandas frame
                                # save data with index column 
                                # replace the original file with newly saved file
                                # import saved file to dynamodb, index column becomes PK
                            ######

                            response=convert_file_with_index(s3_bucket,s3_key,path_name, file_name,file_delimiter,s3_key_withoutextension)
                            #return {
                                #"converted_file":converted_file,
                                #"converted_pathname":converted_pathname,
                                #"target_bucket":target_bucket
                            #}
                            converted_s3_key=response['converted_file']
                            converted_path_name=response['converted_pathname']
                            converted_s3_bucket=response['target_bucket']
                            print(response)
                            # data in converted_s3_key will be imported to dynamodb
                            # note: attribute definitions can only contain columns that are key attributes in 
                            # base table and GSI, LSI
                            
                            dynamodb_table={
                                'TableName': tablename,
                                'AttributeDefinitions': [
                                    {
                                        'AttributeName': 'AutoID',
                                        'AttributeType': 'N'
                                    },
                                    {
                                        'AttributeName': 'Date',
                                        'AttributeType': 'S'
                                    },
                                    {
                                        'AttributeName': 'GEO',
                                        'AttributeType': 'S'
                                    }
                                ],
                                'KeySchema': [
                                    {
                                        'AttributeName': 'AutoID',
                                        'KeyType': 'HASH'
                                    }
                                ],
                                'BillingMode': 'PROVISIONED',
                                'ProvisionedThroughput': {
                                    'ReadCapacityUnits': 10,
                                    'WriteCapacityUnits': 10
                                },
                                'GlobalSecondaryIndexes': [
                                    {
                                        'IndexName': 'priceindex_gsi',
                                        'KeySchema': [
                                            {
                                                'AttributeName': 'Date',
                                                'KeyType': 'HASH'
                                            },
                                            {
                                                'AttributeName': 'GEO',
                                                'KeyType': 'RANGE'
                                            },
                                        ],
                                        'Projection': {
                                            'ProjectionType': 'INCLUDE',
                                            'NonKeyAttributes': [
                                                'Products',
                                                'VALUE',
                                                'STATUS'
                                            ]
                                        },
                                        'ProvisionedThroughput': {
                                            'ReadCapacityUnits': 10,
                                            'WriteCapacityUnits': 10
                                        }
                                    },
                                ]
                            }
                            response=import_table_dynamodb(converted_s3_bucket,converted_path_name,s3_key_extension,file_delimiter,tablename,dynamodb_table,row_number)
                            #if no error from dynamodb
                            # means loading completes
                            loading_status=response['loading_status']
                            print("loading_status is {}".format(loading_status))
                            if loading_status==1:
                                #means loading succeeded
                                print('loading succeeded!')
                                ##### move the file into another s3 bucket for backup ######
                                stage=3
                                backup_file(s3_bucket,s3_key,file_name)
                                #=================================
                                delete_message(message)
                                #=================================
                                exit()
                            else:
                                #loading not complete due to some reason
                                #lambda failed to consume the SQS message
                                messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                                print('Adding failed file into reprogress response list')   

                    else: # the file is not csv, txt 
                            # or the file is a converted file being processed by another lambda
                        print ('the file of {} is not for Data ETL or the converted file is being processed by another lambda.'.format(s3_key))
                        delete_message(message)
                        exit()
            except Exception as e:
                if stage==0 : 
                    print_content='stage={}, error when checking files in s3, description: {}'.format(stage,e)
                if stage ==1:
                    print_content='stage={}, error when splitting large files, description: {}'.format(stage,e)
                if stage ==2:
                    print_content='stage={}, error when loading from s3 to dynamodb, description: {}'.format(stage,e) 
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
#==========================================================================
#==========================================================================
#loading_status=import_table_dynamodb(converted_s3_bucket,converted_path_name,s3_key_extension,file_delimiter,dynamodb_table,row_number)
def import_table_dynamodb(s3_bucket,path_name,s3_key_extension,file_delimiter,tablename,dynamodb_table,row_number):   
   
    response = dynamodb_client.import_table(
        S3BucketSource={
            'S3Bucket': s3_bucket,
            'S3KeyPrefix': path_name
        },
        InputFormat=s3_key_extension.upper(),
        InputFormatOptions={
            'Csv':{
                'Delimiter':file_delimiter
            }          
        },
        InputCompressionType='NONE',
        TableCreationParameters= dynamodb_table
    )
    print('----')
    print('Importing data from "{}" to dynamodb table "{}"'.format(path_name,tablename))
    print(response)
    #keep checking import status 
    attempts=0
    MAX_WAIT_CYCLES=55
    # lambda timeout should be more than (time.sleep) * MAX_WAIT_CYCLES
    # 10 seconds * 55 cycles= 550seconds
    # lambda can wait up to 550s
    # but if there is result before 550s
    # lambda will break the loop and run the next line
    # MAX_WAIT_CYCLES is based on WCUs, data quantity defined at table creation
    loading_status=0

    while attempts < MAX_WAIT_CYCLES:
        attempts += 1
        time.sleep(10)  
        try:
            response=dynamodb_client.describe_table(
                TableName=tablename
            )
            status=response['Table']['TableStatus']
            #'CREATING'|'UPDATING'|'DELETING'|'ACTIVE'|'INACCESSIBLE_ENCRYPTION_CREDENTIALS'|'ARCHIVING'|'ARCHIVED',
            print("table status is {}".format(status))
            if status=='ACTIVE':
                loading_status=1
                break
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pass
    print(loading_status)
    return {
        "loading_status":loading_status
    }
#==========================================================================
def convert_file_with_index(source_bucket_name,source_s3_key,path_name,file_name,file_delimiter,source_s3_key_withoutextension,target_bucket_name=backup_bucket,target_foldername=backup_folder,target_subfoldername=today):
    if 'price' in source_s3_key_withoutextension or 'index' in source_s3_key_withoutextension:
        # source_s3_key is the original key name without any conversion
        # source_s3_key_withoutextension is in lower case
        
        # only files that needs index column can be added index column
        # like table of 'priceindex', pk consists of 3 attributes
        # to add index column as pk in dynamodb table

        resp = s3_client.get_object(Bucket=source_bucket_name, Key=source_s3_key)
        data = pandas.read_csv(resp['Body'],sep=file_delimiter)
        data.index=data.index+1
        # to make index starting from 1
        data.index.name = 'AutoID'
        csv_buffer=StringIO()
        data.to_csv(csv_buffer)
        # 1 to save the converted file into backup folder in backup bucket
        # 2 to import data in converted file to dynamodb
        respon=finding_folder(target_bucket_name,target_foldername,target_subfoldername)
        # respon=target_folder
        # that is the folder, the converted file will be saved.

        save_as='{}converted_{}'.format(respon,file_name)
        s3_session_resource.Object(target_bucket_name,save_as).put(Body=csv_buffer.getvalue())
        csv_buffer.truncate(0) 
        # to backup the original file after loading succeeded
        # can't delete the original file as we can't be sure
        # the following loading process successful or not
        converted_file=save_as
        converted_pathname=respon[0:-1]
        # deleting the trailing '/'
        target_bucket=target_bucket_name
    else:    
        # at the end, all files, converted or not, will be assigned a new parameter as 'converted file'
        converted_file=source_s3_key
        converted_pathname=path_name
        target_bucket=source_bucket_name
    return {
        "converted_file":converted_file,
        "converted_pathname":converted_pathname,
        "target_bucket":target_bucket
    }
#==========================================================================    
def split_file(source_bucket_name, source_s3_key,file_delimiter,file_size_in_mib,target_bucket_name=backup_bucket,target_foler=backup_folder):   
    
    resp = s3_client.get_object(Bucket=source_bucket_name, Key=source_s3_key)
    data = pandas.read_csv(resp['Body'],sep=file_delimiter)
    #data_for_loading =data[['Date','GEO','DGUID','Products','UOM','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','VALUE','STATUS','SYMBOL','TERMINATED','DECIMALS']]

    data.index=data.index+1
    row_number=len(data.index)
    # to make index starting from 1
    file_name=os.path.basename(source_s3_key)
    length=len(file_name)
    s3_key_extension=file_name[length-3:]

    # below is to split the large data files into smaller one
    # Please remember there is no way to save any files in the upload s3 bucket
    # as s3 bucket will trigger sqs that will trigger lambda
    # this is an horible infinite loop
    # the split files are all saved in another backup bucket
    small_files_folder=finding_folder(target_bucket_name,target_foler,'{}/{}'.format(today,file_name))

    csv_buffer=StringIO()
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
        # it can't truncate itself for the next split file data
    return {
        "small_files_folder":small_files_folder
    }
#==========================================================================
#respon=finding_folder(the_hard_code_bucket,the_hard_code_folder,today)
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
def get_tablename(target_name,stamp=today):
    correct_tablename=''
    #we can add spelling corrector here if there's possibility of typos in filenames 
    if 'price' in target_name or 'index' in target_name:
        #always be aware of lower cases
        #make names to be in lower cases as soon as possible
        #s3_key_withoutextension already be lower case
        #means the tables schema of webvoir
        table_name='priceindex_{}'.format(stamp)
        existing_tables=dynamodb_client.list_tables()['TableNames']
        #print(existing_tables)
        if table_name in existing_tables:
            print("table of {} already exists".format(table_name))
            response=dynamodb_client.delete_table(
                TableName=table_name
            )
            # testing if the table is deleted or not
            attempts =0
            MAX_WAIT_CYCLES=10
            table_deletion=0
            while attempts < MAX_WAIT_CYCLES:
                attempts += 1
                time.sleep(3) 
                existing_tables=dynamodb_client.list_tables()['TableNames']
                if table_name not in existing_tables:
                    print("table is deleted successfully")
                    # means table has been deleted
                    table_deletion=1
                    break
                
            if table_deletion==0:
                #measn table can't be deleted
                print("Error when trying to delete it. Can't continue to import data.")
                return {
                    "correct_tablename":"error"
                }

        correct_tablename=table_name   
        print(correct_tablename)
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


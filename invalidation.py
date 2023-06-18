#### Below is to use lambda function (in Python) 
#### to check and initiate Cloudfront Invalidation
from __future__ import print_function
import boto3
import os
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

cloudfront_client = boto3.client('cloudfront')
lambda_client=boto3.client('lambda')
s3_client=boto3.client('s3')
s3_resource=boto3.resource('s3')
session=boto3.Session()
s3_session_resource=session.resource('s3')

aws_region=os.getenv('aws_region')
Account_Id=os.getenv('Account_Id')
topic_name_on_success=os.getenv('topic_name_on_success')
topic_arn_on_success=os.getenv('topic_arn_on_success')
topic_name_on_failure=os.getenv('topic_name_on_failure')
topic_arn_on_failure=os.getenv('topic_arn_on_failure')

time_zone=timezone('EST')
time_zone1="America/New_York"
#print("now is {}".format(datetime.now(time_zone).date()))
today = datetime.now(time_zone).date()
#this can make sure we use est timezone
# --------------- Main handler ------------------
def lambda_handler(event, context):
    '''
    once files are uploaded into s3
    S3-->SQS-->lambda-->cf invalidaton
    if the uploaded file matching caching policy
     and it has older s3 version
        then, continue with cf invalidation
        otherwise, lambda won't do anything as the file was not cached at all
    '''
    stage=0
    if event:
        
        print ("event is {}".format(event))
        message=event['Records'][0]
        messages_to_reprocess = []
        batch_failure_response = {}
        for record in event["Records"]:
            try:    
                body = record["body"]
                body=json.loads(body)
                
                s3_event=body['Records'][0]
                event_name=s3_event['eventName']
                print("event_name is {}".format(event_name))
                if "ObjectCreated:" not in event_name:
                    print('event is not s3:create')
                    #=================================
                    delete_message(message)
                    #=================================
                    exit()
                    #do nothing as this is not a valid event, delete the sqs message 
                    # to avoid reprocessing the invalid message
                else:
                    s3_bucket=s3_event['s3']['bucket']['name']
                    #print("s3_bucket is : {}".format(s3_bucket)) 
                    s3_key=s3_event['s3']['object']['key']
                    s3_key=unquote_plus(s3_key)
                    # to eliminate the '+' sign in the folder name
                    # AWS s3 or sqs automatically substitutes the space in filepath with a "+" sign....
                    # if we use s3_key as prefix, "+" sign will cause error,
                    # the s3_key should be exactly the same as the uploaded file itself (without "+" sign)
                    # if we use s3_key as path parameter in cloudfront_client.create_invalidation,
                    # we must use that "+" sign to indicate this is a path.
                    # in fact, there is no concept of path in AWS s3, i guess AWS use "+" sign 
                    # to tell when the file name is used as a path
                    # for files who doesnot include any space in their path and names, there is no such worries...
                    # we don't know if all uploaded files/folders have space in their names or not
                    # it's better to use the above line to prevent such errors in future
                    print("s3_key is : {}".format(s3_key)) 

                    # the next is to get 
                    # 1 if this file matches caching policy
                    # 2 if this file was updated before
                    # as s3 bucket versioning has been enabled
                    # files with an older version or more than one version 
                    # is not a brand new file, it's possible that it has been cached
                    # if this file also matches caching policy
                    # this file must be invalidated after upload

                    print ("s3_event is {}".format(s3_event))

                    #to check if the file matches caching policy:
                    # the path pattern in cloudfront decides which path is accepted for cache
                    # files in that path (for example: folder of /images/* )should be check here
                    if "images" not in s3_key:
                        print('The file is not in the cache folder')
                        #=================================
                        delete_message(message)
                        #=================================
                        exit()
                    else: 
                        print ("The file is in the cache folder. Continue to check its versions.")

                        # get if there's older version(s)
                        res=check_versioning(s3_bucket,s3_key)
                        if res == "true":
                            #means old version exists
                            print("The file of {} has multiple versions".format(s3_key))
                        else:
                            print ("The file of {} is a new file. No need for CloudFront Invalidation.".format(s3_key))
                            #=================================
                            delete_message(message)
                            #=================================
                            exit()
                        


                        # stage 0 checking completed before invalidation
                        stage=1
                        # start cf invalidation
                        #1 find cf id 

                        cf_id = get_cloudfront_distribution_id(s3_bucket,aws_region)
                        if not cf_id :
                            #means this s3 has no cf...
                            print ("The bucket of {} has no CloudFront.".format(s3_bucket))
                            #=================================
                            delete_message(message)
                            #=================================
                            exit()
                        else:
                            # do the invalidation
                            # the s3_key we get from sqs message contains the full address
                            # now we need to add "/" in the front
                            # WHAT'S MORE, IF THE FILE PATH CONTAINS SPACE
                            # the create_invalidation can't recognize the file path....
                            # the space should be replaced with "+" sign by sqs message
                            # just to refresh the s3_key value from sqs message
                            # 
                            s3_key=s3_event['s3']['object']['key']
                            # to get s3_key with "+" sign
                            if not s3_key.startswith('/'):
                                s3_key = '/' + s3_key
                            try:
                                invalidation = cloudfront_client.create_invalidation(DistributionId=cf_id,
                                    InvalidationBatch={
                                    'Paths': {
                                        'Quantity': 1,
                                        'Items': [s3_key]
                                        # here, to use the file as path by adding "+" sign!!
                                    },
                                    'CallerReference': str(time.time()).replace(".", "")
                                })

                                print("Submitted invalidation. ID {} Status {}".format(invalidation['Invalidation']['Id'],invalidation['Invalidation']['Status']))
                            except Exception as e:
                                #print("Error processing object {} from the bucket {}. Event {}".format(s3_key, s3_bucket, json.dumps(event, indent=2)))
                                print("Error processing object {} from the bucket {}. Error {}".format(s3_key, s3_bucket, e))
                                
                                #failed message will be sent back to sqs
                                messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                                #raise e


            except Exception as e:
                print_content="error occurred during stage {}: {}".format(stage, e)
                print(print_content)
                messages_to_reprocess.append({"itemIdentifier": record['messageId']})
                print ("adding failure message into list...")
                subject=print_content
                body=e
                notify (0,subject,body)

        batch_failure_response["batchItemFailures"] = messages_to_reprocess
        # this is extremely important for using lamba with SQS
        # if the batch size in sqs is more than 1, take 5 for example
        # if lambda failed to consume 1 of these 5 messages, but succeeded in the other 4 messages,
        # lambda will report the failure to sqs, and sqs will consider the whole queue to be a failure..
        # which means the sqs will reput these 5 messages in the queue again about some time ( which is visibility timeout)
        # the same lambda or another lambda will reprocess these 5 messages. This can be disaster if the project can't 
        # accept any duplication operations.
        # AWS provides partial failure feedback in lambda setting. It allows lambda to return the failed messages to sqs.
        # SQS will delete the successful messages in the queue and only allow the failure ones to reprocessed later.
        # This is not yet finished...
        # Setting up this parameter in Lambda makes no changes at all.
        # we need to write very explicitely in lambda: to tell lambda to collect any failure message ID when 
        # it works with sqs.
        # at the very end of the lambda function, remember to return the failure response to SQS.

        # note: try block in lambda won't let lambda exit itself 
        # codes will end here anyway...
        # there is no worry that lambda will stop execution by "except" block in the above and 
        # failure messages won't be returned to sqs
        if len(messages_to_reprocess) ==0:
            print('There is no error message. The file has been processed successfully.')
            #means the list is empty
            return batch_failure_response
        else: #means there's message in the list
            list_to_string=' '.join(map(str, messages_to_reprocess))
            print('There''s error message :{}'.format(list_to_string))
            print('returning failed file to sqs...')
            return batch_failure_response
                         
#=====================================================================================
#to delete sqs message after a successful loading
def delete_message(message):
    #print("message for deletion: {}".format(message))
    sqs_region, sqs_Account_Id, sqs_name = message['eventSourceARN'].split(':')[3:6]
    sqs_client = boto3.client('sqs', region_name=sqs_region)
    sqs_url = f'https://{sqs_region}.queue.amazonaws.com/{sqs_Account_Id}/{sqs_name}'
    #sqs_client.delete_message(QueueUrl=sqs_url, ReceiptHandle=message['receiptHandle'])
    
    print('sqs message deleted')    
    print("QueueUrl is {} , and ReceiptHandle is {}".format(sqs_url,message['receiptHandle']))
            
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
        # if we wish to send different messages to mobile and email
        # we can set as below:
        # Message = json.dumps({'default': subject}),
        # MessageStructure='json'
    )
    return "message sent"

def check_versioning (bucket_name, target_name):
    '''  
    This function use paginator to retrieve info:  
    response = client.list_object_versions(
        Bucket='string',
        Delimiter='string',
        EncodingType='url',
        KeyMarker='string',
        MaxKeys=123,
        Prefix='string',
        VersionIdMarker='string',
        ExpectedBucketOwner='string'
    )
    
    paginator = client.get_paginator('list_object_versions')
    response_iterator = paginator.paginate(
        Bucket='string',
        Delimiter='string',
        EncodingType='url',
        Prefix='string',
        ExpectedBucketOwner='string',
        PaginationConfig={
            'MaxItems': 123,
            'PageSize': 123,
            'StartingToken': 'string'
        }
    )
    '''

    print("checking for object versioning:")
    # create a paginator
    paginator = s3_client.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket=bucket_name,Prefix=target_name)
    
    count=0
    for page in page_iterator:
        for version_id in page['Versions'][0]['VersionId']:
            if version_id:
                count=count+1
    if count >1 :
        print("The file of {} has a total of {} versions.".format(target_name,count))
        return "true"
    else: 
        #after search for all info, no older version
        #print ("The file is new to S3. No need for Invalidation.")
        return "false"

def get_cloudfront_distribution_id(bucket_name,aws_region):
    
    bucket_origin = bucket_name + '.s3.' + aws_region + '.amazonaws.com'
    cf_id = None

    # Create a Paginator
    paginator = cloudfront_client.get_paginator('list_distributions')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate()
    for page in page_iterator:
        for distribution in page['DistributionList']['Items']:
            for cf_origin in distribution['Origins']['Items']:
                    print("Origin found {}".format(cf_origin['DomainName']))
                    if bucket_origin == cf_origin['DomainName']:
                            cf_id = distribution['Id']
                            print("The CF distribution ID for {} is {}".format(bucket_name,cf_id))

    return cf_id


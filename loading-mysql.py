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
'''
#======================================
#below are scripts for leader lambda(parent):
#1 to validate s3 event, s3 key 
#2 to check the file and split the file if it exceeds the limit
#3 to decide to invoke ECS container for Database Init or to invoke child lambda for Data ETL/Analysis
#4 ----waiting for child response -------
#5 to backup the data file in another s3 bucket 
#6 return failure message to SQS only
#======================================
#below are scripts for loading lambda (Child):
#1 to get task from parent 
#2 to connect Database and load data using Procedures (predefined by ECS task)
#3 to check remaining files 
#4 to run procedures for reporting
#5 to return loading status and errors (if any) to parent lambda


from __future__ import print_function

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

### for read/split data files in s3 ###
import pandas 
from io import StringIO
import math

### for MySQL connection ###
import sys
import logging
import pymysql
import json

### clients settings ###
import boto3
import botocore.session as bc
lambda_client=boto3.client('lambda')
s3_client=boto3.client('s3')
s3_resource=boto3.resource('s3')
session=boto3.Session()
s3_session_resource=session.resource('s3')
rds_data_client = boto3.client('rds-data') # this is for aurora only... 

### Environment Variables ###
aws_region=os.getenv('aws_region')
Account_Id=os.getenv('Account_Id')
mysql_database=os.getenv('mysql_database')
mysql_host=os.getenv('mysql_host')
topic_name_on_success=os.getenv('topic_name_on_success')
topic_arn_on_success=os.getenv('topic_arn_on_success')
topic_name_on_failure=os.getenv('topic_name_on_failure')
topic_arn_on_failure=os.getenv('topic_arn_on_failure')
secret_name=os.getenv('secret_name')
#lambda will not use master user to login
# it will use another user dedicated for its job

#below for backup folder 
backup_bucket=os.getenv('backup_bucket')
backup_folder="backup"

### time_zone settings ###
time_zone=timezone('EST')
time_zone1="America/New_York"
#print("now is {}".format(datetime.now(time_zone).date()))
today = datetime.now(time_zone).date()
#this can make sure we use est timezone

### timeout settings ###
time_interval_in_minutes=3600 #  to load the file if it is uploaded within 30 minutes
lambda_timeout_in_seconds=60
maxerrors_allowed=5
 
### report building ###
report_file_prepare=False
#once data files for reporting were loaded into MySQL,
#report building will be initiated
#dont need to wait until all files were loaded completely

# create db connection outside of lambda_handler
# so that to reuse the connection
#print('fetching secret...')

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


mysql_host_name=mysql_host[0:-5]
#print(mysql_host_name)
try:
    conn = pymysql.connect(host=mysql_host_name, user=user_name, password=pass_word, db=mysql_database, connect_timeout=10)
except pymysql.MySQLError as e:
    print("ERROR: Unexpected error: Could not connect to MySQL instance.")
    print(e)
    sys.exit()
#print("SUCCESS: Connection to RDS MySQL instance succeeded")

def lambda_handler(event, context):

    stage =0
    loading_status=0
    reload=False  # reload if there is successful loading within a fixed time
    report=False  # reporting process is not started by default
    report2=False # if there is a second series of reports 

    # after the loading lambda is invoked by leader lambda
    # let loading lambda to check if the file exists.
    # the reason to do so is that when SQS put the message in the queue, it does not check 
    # if the file exists in s3 upload bucket, the file might be processed and removed to backup s3 bucket
    # by another lambda, but the message remains in the queue.
    
    s3_bucket=event['s3_bucket']
    s3_key=event['s3_key']
    file_split=event['file_split']
    small_files_folder=event['small_files_folder'] 
    schema_name=event['schema_name']
    temp_tablename=event['temp_tablename']
    tablename=event['tablename']
    file_delimiter=event['file_delimiter']
     
    try:
        
        res=check_remaining_files(s3_bucket,s3_key)

        if res=="true": 
            #means s3_key exists
            print ('The file of {} exists. Start loading.'.format(s3_key))
        else:
            print_content="There is no such file {}. It might be processed and moved to backup by another lambda.".format(s3_key)
            print(print_content)
            loading_status=2
            return {
                "loading_status":2,
                "error":print_content
            }
        # start loading
        # 1 to find out the source data file is splitted or not
        #print('file_split is {}'.format(file_split))
        if file_split==0:
            # the file is small than 6 mib
            # loading from upload bucket
            file=s3_resource.Object(s3_bucket,s3_key)
            filedata=file.get()['Body'].read()
            row_number=filedata.decode('ISO-8859-1').count('\n')-1
            #print('row_number is {}'.format(row_number))
            total_rows=row_number
            #get the total rows in the original data file
            '''
            below is another way to count row numbers but less fault tolerant
            response=s3_client.get_object(Bucket=s3_bucket,Key=s3_key) 
            data=pandas.read_csv(response['Body'])
            row_number=len(data.index)
            total_rows=row_number
            print('row_number is {}'.format(row_number))
            '''
            # I used several ways to avoid duplicate or incomplete loading:
            #1. to upsert instead of insert
            #2. to check if there is already a successful loading within 30 minutes
            #3. to create log tables in MySQL to record the number rows affected by loading
            #4. to count the row number in temp table before loading
            #5. to count the row number in data file in s3 using lambda
            #6. to compare both numbers (#4 and #5) and decide to load or not
            # in fact, we don't need to apply all above steps
            # I created the template for every step for reference only

            ##### AAA) check status before loading ######
            stage=1
            parameter_for_result='@checking_status'
            usp_name="{}.sp_lambda_loading_check_status('{}','{}',{},{})".format(schema_name,schema_name,tablename,time_interval_in_minutes*60,parameter_for_result)     
            sql_text="call {};".format(usp_name)

            sql_check_text='select 1;'
            run_sql(sql_text,sql_check_text,conn,1)
            # the parameter of '1' means no need to wait for query to finish
            # as this procedure usually runs very fast
            sql_text="select {};".format(parameter_for_result)
            result=run_sql_to_get_result(sql_text,conn)
            if result ==-1:
                #means error when running SP itself
                #ATTENTION: even error when checking loading status
                # there are more measures loading lambda will take to ensure that the loading is not duplicate
                # here,  we can send email via SNS to person in charge , no need to stop and exit lambda 
                print_content="error occurred when executing sp_lambda_loading_check_status. Please check log_for_loading in MySQL."
                print(print_content)
                subject=print_content
                body=''
                #res = notify(0, subject, body)
                
            if result==0 :
                #means don't load, there was a successful loading not long ago
                #exit lambda function
                print_content="there is a successful loading less than 30 minutes ago. Loading paused. Please check."
                print(print_content)
                loading_status=0
                return {
                    "loading_status":loading_status,
                    "error":print_content
                }
                
            if result==1 :
                #means continue to load
                print("After checking, continue to load...")
            
            #below is to call SP in MySQL 
            #1 to create a temp table
                # check for returning result from SP
            #2 ! to load data into this temp table 
            #3 to check the number of rows loaded
            #4 to compare with total_rows to see if there is any difference
            #5 if no, it means all data in the data file were loaded into temp table
            #6 to upsert data into permanent table from temp table
            #7 to write log for loading
            #====================================================================
            ##### BBB) create temp table ######
            stage=2
            parameter_for_result='@temp_table_created'
            usp_name="{}.sp_loading_create_temp_table('{}','{}','{}',{})".format(schema_name,schema_name,temp_tablename,tablename,parameter_for_result)     
            sql_text="call {};".format(usp_name)

            sql_check_text="select status from log_for_loading where locate('temp table creation',EventSource)>0 and timediff(now(),Time_stamp)<10;"
            # trying to get updated log info from log table
            # once Procedure writes log, no matter status is success or not,
            # means the procedure complete running
            run_sql(sql_text,sql_check_text,conn)
            
            sql_text="select {};".format(parameter_for_result)
            result=run_sql_to_get_result(sql_text,conn)  
            if result == -1 :
                #means tempt table creation failed
                print_content="temp table of {} can't be created.".format(temp_tablename)
                print(print_content)
                loading_status=0
                return {
                    "loading_status":loading_status,
                    "error":print_content
                }

            else:
                # temp tables created successfully 
                # or sp is still running (which is rare, but possible)
                # lambda continues
                #====================================================================
                ##### CCC) loading from data file to temp table using lambda ######  
                stage=3 
                load_data_table_mysql(s3_bucket, s3_key,file_delimiter,schema_name,temp_tablename,conn)
                
                sql_text='select count(*) from {}.`{}`;'.format(schema_name,temp_tablename)
                count_temp=run_sql_to_get_result(sql_text,conn) 
                print('after couting rows in temp table, result. is {}'.format(count_temp))
                total_rows=read_data_from_s3(s3_bucket,s3_key,file_delimiter,temp_tablename)
                #====================================================================
                ##### DDD) loading to general table and split into sub tables as well ###### 
                stage=4  
                parameter_for_result='@data_loading_splitting'
                usp_name="{}.sp_loading_PriceIndex('{}','{}','{}','{}',{},{},{})".format(schema_name,schema_name,temp_tablename,tablename,s3_key,total_rows, maxerrors_allowed, parameter_for_result)     
                sql_text="call {};".format(usp_name)
                
                sql_check_text="select status from log_for_loading where (locate('loading',EventSource)>0 or locate('splitting',EventSource)>0)and timediff(now(),Time_stamp)<10;"
                # trying to get updated log info from log table
                # as this procedure may have up to 3 different rows in column eventsource
                # once Procedure writes log, no matter status is success or not,
                # means the procedure complete running
                run_sql(sql_text,sql_check_text,conn)
            
                sql_text="select {};".format(parameter_for_result)
                current_loadingstatus=run_sql_to_get_result(sql_text,conn)  
                # -1 --> error when running sp
                # 0 --> loading failed
                # 1--> loading successful
                if current_loadingstatus==0 or current_loadingstatus==-1: 
                    loading_status=0
                    # means loading status is failure
                    # or not finised yet (rare, but possible)
                    # if we dont have a precise answer to loading status
                    # we can't move the data file to another s3 bucket for back up
                    # the solution is to let another lambda to reprocess the message later
                    # after trying several times, if still can't get a result from this sp
                    # the message will go to dead letter queue which is monitored CloudWatch Alarm 
                    print_content="error occurred during data loading and splitting for table of {}.".format(tablename)
                    print(print_content)
                    return {
                        "loading_status":loading_status,
                        "error":print_content
                    }
                else:
                    loading_status=1
                    stage=5
                    #means loading is successful 
                    # return loading_status as 1 
                    # we can't exit the function now, we need to do reporting in the next steps
                    respo=check_remaining_files(s3_bucket)                
                    print('after checking')     

                    if respo['unloadfiles']==None:
                        #means the function returned an empty unload_file
                        print_content='today''s files were all loaded!!!'
                        print(print_content)
                        subject=print_content
                        body="hooray"
                        notify(1,subject,body)
                        #if today's files are not report-related files,
                        # SPs in mysql won't be executed
                        if ('PriceIndex' in tablename) :
                
                            report=True
                        elif ('here is the tablename' in tablename):
                            report2=True
                    
                    else:
                        print('There are {} files remaining: {}'.format(respo['filenumber'],respo['unloadfiles']))
                        file_string=respo['unloadtables'] 
                        #file_string is a list, needs to convert to string
                        files_to_string=respo['unloadtables']
                        #files_to_string=' '.join(map(str, file_string))
                        files_to_string=files_to_string.lower()
                        
                        print('unload tables are {}'.format(files_to_string))
                        if ('PriceIndex' in tablename) and ('PriceIndex' not in files_to_string) :
                            # means data file needed for reporting was uploaded today, but it has been loaded and moved to back up
                            # no matter what remaining files in s3 now, the report can be built
                                print('report tables for dashboards are ready')
                                report=True
                    if report==True:
                        print('Generating report data.')
                        report_file_prepare=True
                        #tell sql funtion not to wait until status=complete
                        print('stage is {}.Preparing data for reports'.format(stage))
                        ##aaa) to create table for reporting first
                        # reporting procedure only updates the log table when error
                        # so i won't check for log table while mysql is executing the SP
                        report_table_name='1.report'
                        parameter_for_result='@table_for_reporting'
                        usp_name="{}.sp_reporting_50_general_table_for_report_building('{}','{}','{}',{})".format(schema_name,schema_name,tablename,report_table_name,parameter_for_result)     
                        sql_text="call {};".format(usp_name)
                        
                        sql_check_text="select ProcedureName from {}.log_for_reporting where locate('sp_reporting_50_general_table_for_report_building',ProcedureName)>0  and timediff(now(),Time_stamp)<10;".format(schema_name)
                        # trying to get updated log info from log table
                        # once Procedure writes log, no matter status is success or not,
                        # means the procedure complete running
                        # sql 5.7 and 8.0 use different syntax to rename columns
                        # change to the right syntax in .sql file for db init, otherwise error throws
                        run_sql(sql_text,sql_check_text,conn)

                        sql_text="select {};".format(parameter_for_result)
                        result=run_sql_to_get_result(sql_text,conn)  
                        if result ==1: 
                            print_content='table {} for reporting is ready.'.format(report_table_name)
                            print(print_content)
 
                        else: 
                            print_content='table for reporting can''t be created.'
                            print(print_content)
                            subject=print_content
                            body=''
                            #notify(0,subject,body)
                            #although reporting failed, loading successful
                            loading_status=1
                            return {
                                "loading_status":loading_status,
                                "error":print_content
                            }
                        
                        #bbb) now to create report data finally
                        parameter_for_result='@final_row_number'
                        paremater_delimiter=','
                        # this delimiter what usp user uses to separate multiple values for their input parameters
                        year=1995
                        month=12
                        geo_limit='Canada'
                        category='food'
                        usp_name="{}.sp_reporting_1_price_by_year_month_geo_category('{}','{}','{}','{}','{}','{}','{}',{})".format(schema_name,schema_name,report_table_name,paremater_delimiter,year,month,geo_limit,category,parameter_for_result)     
                        sql_text="call {};".format(usp_name)
                        
                        sql_check_text='select 1;'
                        run_sql(sql_text,sql_check_text,conn)
                        
                        #sql_text="select {};".format(parameter_for_result)
                        #result=run_sql_to_get_result(sql_text,conn)  
                        if result >0: 
                            loading_status=1
                            print_content='report data is ready.'.format(report_table_name)
                            print(print_content)
                            # the result will be a number tell us how many rows the report data have
                        else:
                            print_content='there is no report data generated. Please check.'
                            print(print_content)
                            subject=print_content
                            body=''
                            #notify(0,subject,body)
                            loading_status=1
                            return {
                                "loading_status":loading_status,
                                "error":print_content
                            }

               
        if file_split==1:
            # the file is splitted and saved in backup bucket
            # go to backup bucket for loading
            s3_bucket=backup_bucket
            # as this project, each data file does not exceed 6 mib
            # this part is omitted
            # the loading process is exactly like the above when there is no splitted small files
            # except that we include all files in the small_file_folder in a loop and load each file in order



    except Exception as e:
        if stage==0 : 
            print_content='stage={}, error when connecting to MySQL, description: {}'.format(stage,e)
            loading_status=0
        if stage ==1:
            print_content='stage={}, can''t check table previous loading status, description: {}'.format(stage,e)
            loading_status=0
        if stage ==2:
            print_content='stage={}, error when building temp table, description: {}'.format(stage,e) 
            loading_status=0
        if stage ==3:
            print_content='stage={}, error when loading to temp table, description: {}'.format(stage,e)
            loading_status=0
        if stage ==4:
            print_content='stage={}, error when loading to general and sub category tables, description: {}'.format(stage,e)
            loading_status=0
        if stage==5:
            print_content='stage={}, loading complete, reporting failed, description: {}'.format(stage,e) 
            loading_status=1
        print(print_content)
        return {
            "loading_status":loading_status,
            "error":print_content
        }
        
    print('loading complete! Returing good news to leader lambda~')
    loading_status=1
    return {
        "loading_status":loading_status,
        "error":''
    }
 

#=======================================================================
# Read CSV file content from S3 bucket (this function is applied)
def read_data_from_s3(bucket_name,target_name,file_delimiter,temp_tablename):
    resp = s3_client.get_object(Bucket=bucket_name, Key=target_name)
    data = pandas.read_csv(resp['Body'],sep=file_delimiter)
    temp_tablename=temp_tablename.lower()
    if 'priceindex' in temp_tablename:
        data_for_loading =data[['Date','GEO','DGUID','Products','UOM','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','VALUE','STATUS','SYMBOL','TERMINATED','DECIMALS']]
        data_for_loading.index=data_for_loading.index+1
        row_number=len(data_for_loading.index)
        # to make index starting from 1

    return row_number
#=====================================================================================
def load_data_table_mysql(bucket_name, target_name,file_delimiter,schema_name,temp_tablename,connection):
    
    # if we wish to modify and select some columns from the s3 file for data loading,
    # boto3 doesnot provide methods for us to do so
    # data we get in a wholistic view can't be selected based on columns
    # unless we extract columns from it and analyse each column to decide if its the one we are going
    # to load into database, and then extract each row and omit the values we dont wish for,
    # recompose the other values into a new row(string)
    # and then insert it into db...

    # other choices are to 1) use AWS service s3-select, which is not free 
    # or 2) we need to make sure the structure of data file the same as table structure in mysql
    # otherwise, error will happen during loading
    # or 3) to use pandas in lambda function, this great package would solve most of problems for 
    # data reading
    
    resp = s3_client.get_object(Bucket=bucket_name, Key=target_name)
    data = pandas.read_csv(resp['Body'],delimiter=file_delimiter,index_col=False)
    # i tried to find a simple way to read from resp["Body"] and fetch each row and load into MySQL
    # however, if there is comma within the value itself, even the value is quoted 
    # the simple way still break one value into two when loading
    # this will cause the error of exceeding column numbers
    # pandas is a more secure way to load data into database line by line

    temp_tablename=temp_tablename.lower()
    # covert name into lower case before searching
    if 'priceindex' in temp_tablename:
        # sometimes, the orginal data files change its order of columns or add more columns
        # we need to make sure the columns we get from data files are in the right order and right number
        # otherwise , the data can't be loaded into db
        column_list=['Date','GEO','DGUID','Products','UOM','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','VALUE','STATUS','SYMBOL','TERMINATED','DECIMALS']
        data_for_loading=data[column_list]    
        #print(data_for_loading)
        # the above can get "useful and right" columns for the tables in MySQL
        # loop through the data frame

        row_index=1 
        # to exclude index column in dataframe, starting from 1
        
        value_string_total=''
        for row_index,row in data_for_loading.iterrows():
            #here %S can't be used with MySQL
            # so "insert into table values(%s,%s,...) is not working"
            value_string_single=''
            for column_index in range(len(column_list)):
                column_name=column_list[column_index]
                value_string_single=value_string_single+str(data_for_loading[column_name].values[row_index])+"','"
            value_string_single=value_string_single[0:-3]
            value_string_single=" ('"+value_string_single+"'), "
            value_string_total=value_string_total+value_string_single
        # after looping all rows in data file:
        value_string_total=value_string_total[0:-2] 
        
        # the data entry for RDS MySQL, must go in this way. insert row by row into database is not possible.
        # the slow performance would cause table lock 
        # when one row is trying to insert to db, the previous row may not be finished yet
        # we have no way but to combine all rows as together and then to load into db in one move
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO {}.{} Values {};".format(schema_name, temp_tablename,value_string_total)
                #print(sql)
                cursor.execute(sql)
                connection.commit()
        except Exception as e:
            print("error when loading data into temp table. Sql query is: {}; Error is :{}".format(value_string_total,e))
            # do nothing when error, just print out the errors
            # will check later in the codes if the number of error rows are acceptable 
    if connection:
        connection.commit()  
    return
#=====================================================================================
def check_remaining_files(bucket_name,target_name=None):
    resp=s3_client.list_objects_v2(Bucket=bucket_name)

    print("checking for remaining files:")
    #print(resp)
    unload_file=None
    invalid_file=None
    unload_table=None
    countvalid=0
    countinvalid=0
    if 'Contents' in resp:
        print("there are remaining files in s3.") 
        
        for OBJECT in resp['Contents']:
            
            key=OBJECT['Key']
            upload_date=OBJECT['LastModified']
            upload_date=upload_date.astimezone(pytz.timezone(time_zone1))
            #print("upload_date is {}".format(upload_date))
            #change datetime from utc to est
            
            # better to compare both time within 10 minutes
            # there might be more than one loadings per day
            now=datetime.now(time_zone)
            now=now.astimezone(pytz.timezone(time_zone1))
            #now=now.replace(tzinfo=pytz.utc)
            #print("now in pytz is {}".format(now))  
            delta=now-upload_date
            delta_in_minutes=(delta.total_seconds())/60
            #print("delta in mimutes: {}".format(delta_in_minutes))
            found_par="false"
           
            if (key.endswith('txt') or key.endswith('csv')) and delta_in_minutes<time_interval_in_minutes:
                #there're more flat files for today
                if target_name!=None: # we need to check s3_key exists or not
                    # here we can't make target file name in lower case, 
                    # s3_key is not converted to lower case
                    # and the (key) we get using this function is not converted either
                    if str(key)==target_name or target_name in str(key): 
                        #means we found the target file 
                        found_par="true"
                        return found_par

                countvalid=countvalid+1
                if countvalid==1: #means this is the first file found:
                    unload_file=str(key) # get the s3 file name first
                    
                    #print('unload_file is {}'.format(unload_file))
                    unload_file=unload_file.lower()
                    respons=get_tablename(unload_file) # get the corresponding table name in mysql
                    # return {
                        #"schema_name":schema_name,
                        #"temp_tablename":temp_tablename,
                        #"correct_tablename":correct_tablename
                    #}
                    unload_table=respons['correct_tablename']
                    #print('unload_table is {}'.format(unload_table))
                    
                else:
                    unload_file=unload_file+','+str(key)
                    respons=get_tablename(unload_file)
                    unload_table=unload_table+','+respons['tablename']
            else:
                countinvalid=countinvalid+1
                if countinvalid==1: #means this is the first file found:
                    invalid_file=str(key)
                else:
                    invalid_file=invalid_file+','+str(key)
        #===============================================================
        #loop complete, we get unload list and invalid list
        
        if invalid_file!=None:
            print("Below {} files of {} are not for loading.".format(countinvalid,invalid_file))

    else:
        unload_file=None
        unload_table=None
    
    return{
        "unloadfiles":unload_file,
        "filenumber":countvalid,
        "unloadtables":unload_table
    }
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
      
        schema_name='your_mysql_schema_name' 
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
def run_sql(sql_text, sql_check_text,connection,report_file_prepare=False):
    if report_file_prepare==True:
        MAX_WAIT_CYCLES=1
        # if call sp to build reports, 
        # there is no need to wait to get a 'complete' status for executing sql statement
    else:
        MAX_WAIT_CYCLES = 20
        # if a sql statement can't finish within 20 time cycles 
        # lamda continue to run the next line
        # the reason for waiting is that if we don't wait for this procedure (aaa) to finish
        # and we run the next SP (bbb), we may not be able to get any correct result
        # because (aaa) hasn't updated the status in log table yet.
    attempts = 0
    print("Executing: {}".format(sql_text))
    
    with connection.cursor() as cur:
        cur.execute(sql_text)  

    #MySQL doesnot have boto3 to describe a sql statement executing status
    # we need to check log table instead
    while attempts < MAX_WAIT_CYCLES:
        attempts += 1
        time.sleep(3)  
        #20cycles * 3 seconds=60s
        # lambda can wait up to 60s
        # but if there is result before 60s
        # lambda will break the loop and run the next line
        status = status_check(sql_check_text,conn)
        if status ==1:
            #means log shows success
            # lambda can continue
            print("status is: completed".format(status))
            break 
        #else: lambda won't wait any more 
        # 
    connection.commit()             
    return 
#==========================================================================        
def status_check(sql_check_text,connection):
    
    with connection.cursor() as cur:
        cur.execute(sql_check_text)
    #record = cur.fetchone()[0]
    # if cur is empty, can't get value for variable 'record'
    if cur.rowcount>0:
        #means log table is updated 
        status=1
    else:
        #means error occurred 
        status=0
    connection.commit()
    return status
#==========================================================================            
def run_sql_to_get_result(sql_text,connection,report_file_prepare=False):
    if report_file_prepare==True:
        MAX_WAIT_CYCLES=1
        # if call sp to build reports, 
        # there is no need to wait to get a 'complete' status for executing sql statement
    else:
        MAX_WAIT_CYCLES = 20
        # if a sql statement can't finish within 20 time cycles 
        # lamda continue to run the next line
        # the reason for waiting is that if we don't wait for this procedure (aaa) to finish
        # and we run the next SP (bbb), we may not be able to get any correct result
        # because (aaa) hasn't updated the status in log table yet.
    attempts = 0
    print("Executing: {}".format(sql_text))
    record=-1
    while attempts < MAX_WAIT_CYCLES:
        attempts += 1
        time.sleep(1)    
        with connection.cursor() as cur:
            cur.execute(sql_text)
        if cur.rowcount>0:
            break  
    
    record = cur.fetchone()[0]
    print('after checking {}, result is {}'.format(sql_text,record))
    connection.commit()    
    return record

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

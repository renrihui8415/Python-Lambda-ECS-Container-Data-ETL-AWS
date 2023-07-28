Python in Lambda, ECS Container

The python scripts for ECS containers will be shared in the repo of 'DockerImage-Project-Website'.
This repo contains Python scripts for :
1) Lambda function that initiates Cache Invalidation for CloudFront (AWS Content Delivery Network).
2) Lambda function that serve as parent function in Data ETL and Data Analysis in MySQL.
3) Lambda function that serve as child function in Data ETL and Data Analysis in MySQL.
4) Lambda function that serve as loading function in Data ETL and Dynamodb Table Creation

=====> For AWS RDS (MySQL):
Once files are uploaded into S3 bucket, SQS will get the S3 notification and trigger lambda to consume the message and process the files.
For Data ETL and Analysis, Parent Lambda checks the files in S3 and invokes Child Lambda or ECS Container according to data quantity.
After getting response from Child Lambda or ECS, Parent Lambda will decide to proceed to the next step or not. If yes, data file in S3 will be moved to another S3 bucket for backup.

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

=====> For Dynamodb Table:
Loading Lambda:
1) to validate s3 event, s3 key
2) to check the file and split the file if it exceeds the limit
3) to convert the file and add new attribute as PK 
4) to delete the dynamodb table if exists
5) to use 'import_table' feature to load data from s3 to dynamodb
6) to check loading status 
7) to backup the data file in another s3 bucket 
8) return failure message to SQS if any

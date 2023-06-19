Python in Lambda, ECS Container

The python scripts for ECS containers will be shared in the repo of 'DockerImage-Project-Website'.
This repo contains Python scripts for :
1) Lambda function that initiates Cache Invalidation for CloudFront (AWS Content Delivery Network).
2) Lambda function that serve as parent function in Data ETL and Data Analysis.
3) Lambda function that serve as child function in Data ETL and Data Analysis.

Once files are uploaded into S3 bucket, SQS will get the S3 notification and trigger lambda to consume the message and process the files.
For Data ETL and Analysis, Parent Lambda checks the files in S3 and invokes Child Lambda or ECS Container according to data quantity.
After getting response from Child Lambda or ECS, Parent Lambda will decide to proceed to the next step or not. If yes, data file in S3 will be moved to another S3 bucket for backup.

Parent Lambda:
1) to validate s3 event, s3 key 
2) to check the file and split the file if it exceeds the limit
3) to decide to invoke ECS container for Database Init or to invoke child lambda for Data ETL/Analysis
4) ----waiting for child response -------
5) to backup the data file in another s3 bucket 
6) return failure message to SQS if any

Child Lambda:
1) to get task from parent 
2) to connect Database and load data using Procedures (predefined by ECS task)
3) to check remaining files 
4) to run procedures for reporting
5) to return loading status and errors (if any) to parent lambda

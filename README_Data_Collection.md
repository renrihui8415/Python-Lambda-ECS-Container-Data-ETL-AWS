#### Data Collection using Automator, Python, and AWS Services 

### '1. data-collection-from-data-source'.py file is to collect and transfer raw data.

### To achieve 100% automation, the file will be added as a TASK in --
### Task Scheduler (for Windows);
### or Automator Applications (for Mac); 
### After we successfully setup the task, this python scripts are 
### run at 8 am every morning (for example) and the latest data is collected 
### and uploaded to the cloud or any data storage you have.
### From there, data is processed in Streaming Platform, Database, or Data Warehouse for further usage. 
### The processed data can be transferred to data storage where reports or dashobards can be built.

### Note: MacOS itself doesnot have a similarity to Task Scheduler in Windows.
### We can find alternatives in the market. The choices are many.


=============> Python Scripts
# To download the file from data source.
  ## The Datasource the website uses is the official website of 'Canada Statistics'

# To save the file in today's folder under the main folder of 'data' locally
# 1) to find today's date
# 2) to get today's folder file path
# 3) to get the file name from the URL
# 4) to get the file path
# 5) to save the file in the path created just now

# To upload the latest data files to s3 in the cloud using AWS CLI
# 6) to get the absolute file path
# 7) to upload the file to s3 bucket under the foler of data/today's date/
# the AWS CLI is like : aws s3 cp "/Users/.../data/2023-07-28/ind-all.json" s3://.../data/2023-07-28/
# 7.1) to construct command:
# 7.2) to execute the command in python

# After the file is uploaded into s3 in the cloud, lambda and ECS will be invoked and work on data processing. 

# The Data will go through ETL to its destination in the database (for this project).

# The report data will be calculated and saved in the s3 where the website hosts. Charts on the website will be updated with the latest data. 

=============> 
**** From the very beginning of data collection until the data analytics publishing, there is no manual process at all.
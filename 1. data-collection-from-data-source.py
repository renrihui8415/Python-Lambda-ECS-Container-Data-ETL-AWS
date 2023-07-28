### Below is to get data from data source using python ####
### To achieve 100% automation, the file will be added as a TASK in --
### Task Scheduler (for Windows);
### Automator Applications (for Mac); 
### After we successfully setup the task, this python scripts is 
### run at 8 am every morning (for example) and the latest data is collected 
### and uploaded to the cloud or any data storage you have.
### From there, data is processed for further usage. Like report/dashboard building.
### as MacOS itself doesnot have a similarity to Task Scheduler in Windows,
### we can find alternatives in the market. The choices are many.

# Required package:
# $ pip install requests

import requests
import os
from datetime import date
import subprocess
URL="https://www150.statcan.gc.ca/n1/dai-quo/ssi/homepage/ind-all.json"
# get the data file URL
# Canada Statistics provides the URL for data files
s3_bucket="here is the name of bucket"
# get the name for upload s3 bucket 
data_folder_path='here is the folder name '

response = requests.get(URL, allow_redirects=True)

# next is to save the file in today's folder under the main folder of 'data'
# 1) to find today's date
today=date.today()  # the format is 2023-07-28
# 2) to get today's folder file path
today_folder="./data/{}".format(today)
    # 2.1) to check today's folder exists or not 
if os.path.isdir(today_folder):
    today_folder_exists=1
    # folder already exists
else:
    today_folder_exists=0
    # 2.2) to create today's folder
    os.makedirs(today_folder)

# 3) to get file name from the URL
    # 3.1) to find the last '/' in the URL
position=URL.rfind('/') # it shows the number of the position where the last '/' is 
print(position)
    # 3.2) to get the file name
file_name=URL[position+1:]
# 4) to get the file path
file_path=today_folder + '/'+file_name

# 5) to save the file in the path created just now
open(file_path, 'wb').write(response.content)

# next is to upload the latest data files to s3 in the cloud using AWS CLI
# 6) to get the absolute file path
absolute_path=os.path.abspath(file_path)
# 7) to upload the file to s3 bucket under the foler of data/today's date/
# the AWS CLI is like : aws s3 cp "/Users/.../data/2023-07-28/ind-all.json" s3://.../data/2023-07-28/
# 7.1) to construct command:
command='aws s3 cp "{}" s3://{}/{}/{}/'.format(absolute_path,s3_bucket,data_folder_path,today)
# 7.2) to execute the command in python
process=subprocess.run(command,shell=True)
# after the file is uploaded into s3 in the cloud, lambda will be invoked and work on data processing
# the data will go through ETL to its destination in DB
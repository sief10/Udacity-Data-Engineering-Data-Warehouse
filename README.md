### Project Overview

##### This Project handles data of a music streaming startup, Sparkify, Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:
  
     - s3://udacity-dend/song_data: static data about artists and songs Song-data .
     - s3://udacity-dend/log_data: event data of service usage e.g. who listened what song, when, where, and with which client .


### Database Model
    Sparkify analytics database (called here sparkifydb) schema has a star design. Start design means that it has one Fact Table having business data, and supporting Dimension Tables. The Fact Table answers one of the key questions: what songs users are listening to. DB schema is the following:
   
   <img src="./ERD.jpg?raw=true" width="600" />
   
### Quick start
    Project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables in AWS Redshift cluster,fetch data from JSON files stored in AWS S3, process the data,and insert the data to AWS Redshift DB.
 
### Project Steps

  - Update dwh.cfg and fill in the open fields.Fill in AWS acces key(KEY) and secret(SECRET).

  - To access AWS, you need to do in AWS the following:
            > Loading Configuration files
            > Create instances from (S3,IAM,Redshift)
            > create IAM user (e.g. dwhuser)
            > create IAM role (e.g. dwhRole) with AmazonS3ReadOnlyAccess access rights
            > get ARN
            > create and run Redshift cluster (e.g. dwhCluster => HOST)

  - For creating IAM role, getting ARN and running cluster, you can use Udacity-AWS-Setup.ipynb.
  
 ### Program Execution 
 
 - Run Udacity-AWS-Setup.ipynb to create IAM User,Rules and Create redshift cluster.
 - Run Create_tables.py to create staging tables and analytics layer .
 - Run etl.py to load the data from S3 buckets and load into staging layer then loading data from staging layer into analytics layer


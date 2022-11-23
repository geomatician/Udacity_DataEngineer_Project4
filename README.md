# Project 4: Data Lake

# Purpose of database in context of Sparkify and their goals

Sparkify wants to analyze data that they are collecting on songs and user activity in their music streaming application. They want to gain insights into the kinds of songs their users are currently listening to.

Currently, all of their data resides within JSON files of two types- song data files and log data files. Song data files contain information about songs and artists. Log data files contain information about application users and times. The analytics team at Sparkify wants to find a better way to organize and query this data to generate analytics. This can be done by setting up an ETL pipeline that 1) reads data from the JSON files in S3, 2) processes the files using Spark running on an EMR cluster on AWS, and 3) loads it into a collection of fact and dimension tables back into S3 in a newly created bucket, after which optimized queries can be run on the database. With an enhanced organization of data, the analytics team can run customized, ad-hoc queries and generate useful analytics from the data. Some of the output files in S3 are partitioned by key fields such as year, month, and artist. 

# How to run the Python scripts

1) You should already have an EMR cluster created in AWS. The EMR cluster should have Spark installed on it. 

2) Provide your AWS credentials for your AWS account where the EMR cluster was created within the config file named dl.cfg.

3) Clone the Git repository to a directory on your local machine. 

4) SSH into the master node of the EMR cluster using ```ssh -i PATH_TO_KEY_PAIR_FILE hadoop@MASTER_NODE_PUBLIC_DNS```

5) Copy over the dl.cfg anf etl.py files from the cloned Git repository onto the master node EC2 instance using:

```scp -i PATH_TO_KEY_PAIR_FILE PATH_TO_FILE_TO_COPY hadoop@MASTER_NODE_PUBLIC_DNS:~/FOLDER_ON_MASTER_NODE_WHERE_TO_COPY```

where:

PATH_TO_KEY_PAIR_FILE is the path to where the private key file (.pem) is located on disk
PATH_TO_FILE_TO_COPY is the path to the file on disk that you want to copy to the master node EC2 instance; this is
    either the path to the dl.cfg file or the etl.py file   
MASTER_NODE_PUBLIC_DNS is the public DNS name of the master node e.g. ec2-35-90-245-54.us-west-2.compute.amazonaws.com
FOLDER_ON_MASTER_NODE_WHERE_TO_COPY is the folder under the master node's root directory where the two files will be copied


6) Then, navigate (```cd```) into the FOLDER_ON_MASTER_NODE_WHERE_TO_COPY directory on the master node and run this command:

```
spark-submit etl.py

```

7) View the output parquet files in the S3 bucket udacitydatalakebucket.

Python 3.9.13 was used for this project. 

# Files in repository

1) etl.py - this will extract the data from the song and log JSON files in S3 and process it using Spark on an EMR cluster into fact and dimension tables, and from there output them back as parquet files in a separate bucket in S3. It will load the files into separate directories for each table within the main 'udacitydatalakebucket' bucket. 
2) dl.cfg - this is a configuration file used to store and retrieve AWS credentials such as the Access Key ID and Secret Access Key.

# Database schema design and ETL pipeline

The data gets initially loaded from the source S3 bucket (udacity-dend) into memory in Spark. Processing is done within Spark to to create the fact table songplays and dimension tables songs, artists, users, and time. Each table has its own primary key that is unique and not null by definition. Some of the tables also have foreign keys that allow them to be associated with other tables in the database: for example, the songplays table has user_id, artist_id, and song_id fields, and the songs table has an artist_id field. With the combination of their primary and foreign keys through joins, customized SQL queries can be written to generate analytics across these tables. 

 DISTINCT constraints have been introduced into the Spark SQL select statements to prevent duplicates values from being populated into the fact and dimension tables. Furthermore, when reading from the S3 JSON files, duplicate rows are filtered out. The ETL pipeline contained within etl.py, when run, extracts data from the JSON files, loads it into the fact and dimension tables from Spark data frames, and then from there outputs the data back in S3 in parquet files partitioned appropriately.

The songplays table is the core table (i.e. fact table) that contains foreign keys to the songs, artists, users, and time tables (i.e. dimension tables).  Together, they form a star schema with the songplays table at the center, and the other four tables around it. A star schema in the way organized in this project provides several benefits: 1) it makes queries easier with simple joins 2) we can perform aggregations on our data, and 3) we can relate the fact table with the dimension tables to perform cross-table analysis ; the fact table contains foreign keys which by definition are primary keys of the dimension tables. The dimension tables contain additional information about each type of entity- i.e. song, artist, user, and time, whereas the fact table displays the relationships between these different entities in one table using foreign keys. 

This serves as a justification for the database schema design and ETL pipeline. 


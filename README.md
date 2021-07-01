# INTRODUCTION
### Purpose of the project:
This project is to build an ETL pipeline for a data lake hosted on S3 that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 
### What is Sparkify?
Sparkify is a startup that just launched a music streaming application. The application has collected song and user activities in JSON format. The anylytics team wants to understand what songs users are listening to more easily than looping through all the JSON files. 
### How this project is going to help Sparkify
The data pipeline will allow the analytics team to continue finding insights in what songs their users are listening to. 
# DATABASE SCHEMA DESIGN & ETL PROCESS
### Database Schema Design (Star Schema)
##### **Fact Table**
songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)** - records in log data associated with song plays i.e. records with page NextSong
##### **Dimension Tables**
1. users (user_id, first_name, last_name, gender, level) - users in the app
2. songs (song_id, title, artist_id, year, duration) - songs in music database
3. artists (artist_id, name, location, latitude, longitude) - artists in music database
4. time (start_time, hour, day, week, month, year, weekday) - timestamps of records in **songplays** broken down into specific units
### ETL Process
1. Read song_data and load_data from S3, transforms them to create songs and artists tables, and writes them to partitioned parquet files in table directories on S3.
2. Read log_data and load_data from S3, transforms them to create users, time and songplays tables, and writes them to partitioned parquet files in table directories on S3.
# FILES IN REPOSITORY
* etl.py reads and process files from song_data and log_data and loads them into tables.
* README.md provides discussion on the project
# HOW TO RUN THE PYTHON SCRIPT
* Run python etl.py to read song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

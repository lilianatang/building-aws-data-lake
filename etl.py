import configparser
import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    initiates the spark session to be used throughout the ETL process
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def get_files(filepath):
    """
    gets all json files in a specified filepath and add each file as an element to a list
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

def process_song_data(spark, input_data, output_data):
    """
    reads song_data and load_data from S3, transforms them to create songs and artists tables, and writes them to partitioned parquet files in table directories on S3.
    """
    # get filepath to song data file
    song_data = get_files(input_data + "song-data/")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates().sort("song_id")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(output_data + "songs.parquet")
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates().sort("artist_id")
    
    # write artists table to parquet files
    artists_table.write.mode('append').parquet(output_data + "artists.parquet")

def process_log_data(spark, input_data, output_data):
    """
    reads log_data and load_data from S3, transforms them to create users, time and songplays tables, and writes them to partitioned parquet files in table directories on S3.
    """
    # get filepath to log data file
    log_data =  get_files(input_data+ "log-data/")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table =  df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('append').parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) // 1000))
    df1 = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_date_time = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df2 = df.withColumn("date_time", get_date_time(df.ts))
    df1.createOrReplaceTempView("timestamp_table")
    df2.createOrReplaceTempView("datetime_table")    
    df3 = spark.sql("SELECT timestamp_table.ts AS start_time, datetime_table.date_time,\
          hour(datetime_table.date_time) AS hour, weekofyear(datetime_table.date_time) AS week,\
          month(datetime_table.date_time) AS month, year(datetime_table.date_time) AS year,\
          weekday(datetime_table.date_time) AS weekday, day(datetime_table.date_time) AS day\
          FROM timestamp_table \
          JOIN datetime_table ON timestamp_table.ts = datetime_table.ts")
    # extract columns to create time table
    time_table = df3.select(["start_time", "hour", "day", "week", "month", "year", "weekday"]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(["year", "month"]).parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")
    df.createOrReplaceTempView("log_table")
    song_df.createOrReplaceTempView("song_table")
    final_df = spark.sql("SELECT log_table.ts AS start_time, log_table.userId AS user_id, song_table.song_id, song_table.artist_id, log_table.sessionId AS session_id, log_table.location, log_table.userAgent as user_agent, log_table.level\
          FROM log_table \
          JOIN song_table ON log_table.song = song_table.title")
    final_df = final_df.withColumn("monotonically_increasing_id", F.monotonically_increasing_id())
    window = Window.orderBy(F.col('monotonically_increasing_id'))
    final_df = final_df.withColumn('songplay_id', F.row_number().over(window))
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = final_df.select(["songplay_id", "start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"]).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("start_time").parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

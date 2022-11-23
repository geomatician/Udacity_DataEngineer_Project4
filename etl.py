import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS_CREDS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS_CREDS",
                                                 "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
        This function creates a new SparkSession object, which is the
        entrypoint into all all functionality in Spark. This code will
        create a new SparkSession if it doesn't already exist.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function reads song data from JSON files in S3, and extracts
        the necessary columns used to create the 'songs' and 'artists'
        tables from the JSON files, before writing them out as parquet
        files to another S3 bucket.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    song_df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    song_df.createOrReplaceTempView("songdata")
    songs_table = spark.sql(
        """
            SELECT DISTINCT(song_id), title, artist_id, year, duration
            FROM songdata
        """
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite"). \
        parquet(output_data + "songs/songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql(
        """
            SELECT DISTINCT(artist_id), artist_name, artist_location,
            artist_latitude, artist_longitude
            FROM songdata
        """
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data +
                                                  "artists/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
        This function reads log data from JSON files in S3, and extracts
        the necessary columns used to create the 'users', 'time', and
        'songplays' tables from the JSON files, before writing them out
        as parquet files to another S3 bucket. For the songplays table
        specifically, this function also reads in song data which is then
        joined to the log data in order to populate the songplays table.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    log_df = log_df.filter("page == 'NextSong'")

    # extract columns for users table
    log_df.createOrReplaceTempView("userdata")
    users_table = spark.sql(
        """
            SELECT DISTINCT(userId), firstName, lastName, gender, level
            FROM userdata
        """
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data +
                                                "users/users.parquet")

    # create timestamp column from original timestamp column (in seconds)

    log_df = log_df.withColumn("conv_ts", to_timestamp(col("ts") / 1000))

    # create datetime column from original timestamp column
    log_df = log_df.withColumn('datetime', from_unixtime(col("ts")/1000))

    log_df.createOrReplaceTempView("timedata")

    # extract columns to create time table
    time_table = spark.sql(
        """
            SELECT ts,
            EXTRACT(hour from t.datetime) as hour,
            EXTRACT(day from t.datetime) as day,
            EXTRACT(week from t.datetime) as week,
            EXTRACT(month from t.datetime) as month,
            EXTRACT(year from t.datetime) as year,
            EXTRACT(dayofweek from t.datetime) as dow
            FROM timedata t
        """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        output_data + "time/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json").dropDuplicates()

    # extract columns from joined song and log datasets to create songplays
    # table

    songplays_table = log_df.join(song_df, log_df.artist ==
                                  song_df.artist_name, "inner")

    songplays_table.createOrReplaceTempView("songplaydata")

    songplays_table = spark.sql(
        """
            SELECT ts, userId, level, song_id, artist_id, sessionId, location,
            userAgent, year, month(conv_ts) as month
            FROM songplaydata
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month"). \
        mode("overwrite").parquet(output_data + "songplays/songplays.parquet")


def main():
    """
        This creates a new SparkSession and runs the functions defined
        above to process song and log JSON data files in S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacitydatalakebucket/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

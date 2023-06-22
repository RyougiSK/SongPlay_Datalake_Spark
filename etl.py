import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    """
    Create and return a SparkSession.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data and create songs and artists tables.

    Args:
        spark (SparkSession): The SparkSession object.
        input_data (str): The input data path.
        output_data (str): The output data path.
    """
    # Get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # Read song data file
    df = spark.read.json(song_data)

    # Extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet")

    # Extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")

    # Write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Process log data and create users, time, and songplays tables.

    Args:
        spark (SparkSession): The SparkSession object.
        input_data (str): The input data path.
        output_data (str): The output data path.
    """
    # Get filepath to log data file
    log_data = input_data + "log_data/*"

    # Read log data file
    df = spark.read.json(log_data)

    # Filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # Create datetime column from original timestamp column
    df = df.withColumn("Timestamp", F.from_unixtime(col("ts") / 1000))

    # Find recent user level by finding the last record of each user
    last_order_time = df.groupBy('userId').agg(F.max('Timestamp').alias('last_order_time'))
    df_recent = df.join(last_order_time, on='userId', how='left')
    df_songs_recent = df_recent.filter(df_recent.Timestamp == df_recent.last_order_time)

    # Extract columns for users table
    users_table = df_songs_recent.select("userId", "firstName", "lastName", "gender", "level")

    # Write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")

    # Extract columns to create time table
    time_table = df.select('Timestamp')
    time_table = time_table.withColumn('hour', F.hour('Timestamp'))
    time_table = time_table.withColumn('day', F.dayofmonth('Timestamp'))
    time_table = time_table.withColumn('weekofyear', F.weekofyear('Timestamp'))
    time_table = time_table.withColumn('month', F.month('Timestamp'))
    time_table = time_table.withColumn('year', F.year('Timestamp'))
    time_table = time_table.withColumn('weekday', F.dayofweek('Timestamp'))

    time_table = time_table.selectExpr("Timestamp as start_time", "hour as hour", "day as day",
                                       "weekofyear as weekofyear",
                                       "month as month", "year as year", "weekday as weekday")

    # Write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet")

    # Read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*")
    song_df = song_df.withColumnRenamed("title", "song")

    # Extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, on='song', how='left')
    songplays_table = songplays_table.select("Timestamp", "userId", "level", "song_id", "artist_id", "sessionId",
                                             "location", "userAgent")
    songplays_table = songplays_table.withColumn('year', F.year('Timestamp'))
    songplays_table = songplays_table.withColumn('month', F.month('Timestamp'))

    songplays_table = songplays_table.selectExpr("Timestamp as start_time", "userId as userId", "song_id as song_id",
                                                 "artist_id as artist_id",
                                                 "sessionId as sessionId", "location as location",
                                                 "userAgent as userAgent", "year as year", "month as month")

    # Write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays.parquet")


def main():
    """
    Main entry point of the ETL process.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-target-s3/data_model/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

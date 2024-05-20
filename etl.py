import configparser
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    dayofmonth,
    dayofweek,
    hour,
    month,
    udf,
    weekofyear,
    year,
)
from pyspark.sql.types import TimestampType

# Set logging configuration
logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Read AWS configuration
config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("aws", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("aws", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
    Create and return a Spark session.
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    """
    Process song data files, create songs and artists tables, and save them as parquet files.

    :param spark: Spark session object
    :param input_data_path: Path to input song data
    :param output_data_path: Path for output parquet files
    """
    # Filepath to song data
    song_data_path = input_data_path + "/song_data/*/*/*/*.json"

    # Read song data files
    logger.info("Reading song data JSON files")
    song_df = spark.read.json(song_data_path)

    # Extract columns to create songs table
    songs_table = song_df.select(
        "song_id", "title", "artist_id", "year", "duration"
    ).dropDuplicates()

    # Write songs table to parquet files partitioned by year and artist
    logger.info(
        "Writing songs table partitioned by year and artist_id in parquet format"
    )
    songs_table.write.partitionBy("year", "artist_id").parquet(
        output_data_path + "/songs_table.parquet"
    )

    # Extract columns to create artists table
    artists_table = (
        song_df.select(
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        )
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
        .dropDuplicates()
    )

    # Write artists table to parquet files
    logger.info("Writing artists table in parquet format")
    artists_table.write.parquet(output_data_path + "/artists_table.parquet")


def process_log_data(spark, input_data_path, output_data_path):
    """
    Process log data files, create users, time, and songplays tables, and save them as parquet files.

    :param spark: Spark session object
    :param input_data_path: Path to input log data
    :param output_data_path: Path for output parquet files
    """
    # Filepath to log data
    log_data_path = input_data_path + "log_data/*/*/*.json"  # S3 directory structure
    # log_data_path = input_data_path + "log_data/*.json"           # Local directory structure

    # Read log data files
    logger.info("Reading log data JSON files")
    log_df = spark.read.json(log_data_path)

    # Filter by actions for song plays
    log_df = log_df.filter(log_df.page == "NextSong")

    # Extract columns for users table
    users_table = (
        log_df.select("userId", "firstName", "lastName", "gender", "level")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .dropDuplicates()
    )

    # Write users table to parquet files
    logger.info("Writing users table in parquet format")
    users_table.write.parquet(output_data_path + "/users_table.parquet")

    # Create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp(log_df.ts))

    # Create datetime columns from derived start_time column
    log_df = (
        log_df.withColumn("hour", hour(log_df.start_time))
        .withColumn("day", dayofmonth(log_df.start_time))
        .withColumn("week", weekofyear(log_df.start_time))
        .withColumn("month", month(log_df.start_time))
        .withColumn("year", year(log_df.start_time))
        .withColumn("weekday", dayofweek(log_df.start_time))
    )

    # Extract columns to create time table
    time_table = log_df.select(
        "start_time", "hour", "day", "week", "month", "year", "weekday"
    ).dropDuplicates()

    # Write time table to parquet files partitioned by year and month
    logger.info("Writing time table partitioned by year and month in parquet format")
    time_table.write.partitionBy("year", "month").parquet(
        output_data_path + "/time_table.parquet"
    )

    # Read song data to use for songplays table
    logger.info("Reading song data for join")
    song_df = spark.read.json(input_data_path + "song_data/*/*/*/*.json")
    song_df = song_df.withColumnRenamed("year", "song_year")

    # Extract columns from joined song and log datasets to create songplays table
    songplays_table = (
        log_df.join(song_df, song_df.artist_name == log_df.artist, "inner")
        .withColumn("songplay_id", F.monotonically_increasing_id())
        .select(
            "songplay_id",
            "start_time",
            "userId",
            "level",
            "song_id",
            "artist_id",
            "sessionId",
            "location",
            "userAgent",
            "month",
            "year",
        )
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
    )

    # Write songplays table to parquet files partitioned by year and month
    logger.info(
        "Writing songplays table partitioned by year and month in parquet format"
    )
    songplays_table.write.partitionBy("year", "month").parquet(
        output_data_path + "/songplays_table.parquet"
    )


def main():
    """
    Main function to drive the ETL process.
    """
    spark = create_spark_session()

    # S3 data paths
    input_data_path = "s3a://udacity-dend/"
    output_data_path = "s3a://your-output-bucket/"

    process_song_data(spark, input_data_path, output_data_path)
    process_log_data(spark, input_data_path, output_data_path)
    logger.info("ETL job completed!")


if __name__ == "__main__":
    main()

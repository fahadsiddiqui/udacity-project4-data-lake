# Data Lake with Spark

## Project Overview

Sparkify, a rapidly growing music streaming startup, wants to transition their data warehouse to a data lake. Their data, currently stored in S3, includes JSON logs of user activity on the app and JSON metadata about the songs in their app.

## Project Description

This project aims to construct an ETL pipeline that extracts data from S3, processes it using Spark, and loads it back into S3 as a set of dimensional tables. This will enable Sparkify's analytics team to gain insights into user listening behaviors.

## Run Guide

Ensure you have `pyspark` installed locally or in a virtual environment.

1. Update `dl.cfg` with IAM user credentials that have read and write access to S3.
2. Run `python etl.py` in the terminal to execute the script.

### Local Run on Sample Dataset

To run the script on a sample dataset, follow these additional steps:

1. In the `main` function, comment out the S3 paths and uncomment the local data paths.
2. In the `process_log_data` function, comment out the S3 path and uncomment the local path (the directory structure differs between the local data directory and the S3 bucket).

Note: To re-run locally, you need to remove the content of the `output` directory using `rm -rf 'path/to/output/'`.

## Schema for Song Play Analysis

Using the song and log datasets, a star schema is created and optimized for queries on song play analysis. This includes the following tables:

### Fact Table

**tbl_songplays** - Records in log data associated with song plays, i.e., records with page `NextSong`.

- songplay_id
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

### Dimension Tables

**tbl_users** - Users in the app.

- user_id
- first_name
- last_name
- gender
- level

**tbl_songs** - Songs in the music database.

- song_id
- title
- artist_id
- year
- duration

**tbl_artists** - Artists in the music database.

- artist_id
- name
- location
- latitude
- longitude

**tbl_time** - Timestamps of records in songplays broken down into specific units.

- start_time
- hour
- day
- week
- month
- year
- weekday

## Description of Project Files

**dl.cfg**: Contains IAM user credentials used by `pyspark` to read/write files on S3.

**etl.py**: Contains functions to process song and log data from S3 by loading them into Spark dataframes, removing duplicates, manipulating columns, and saving dataframes back to S3 in `parquet` format.

## Author

- [Fahad Siddiqui](https://github.com/fahadsiddiqui)

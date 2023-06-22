
# Project: Data Lake with Spark

## Table of content

- Introduction
- File structure
- Run program
- Database schema design
- Maintainers


## Introduction

Startup Sparkify has its new music streaming app running for months. In order to provides better user experience to challenge the existing competitor Sportify, the analytics team is particularly interested in understanding what songs users are listening to. This project is aiming to build a databased which provides solid data stream for the analytical team.




## File structure

- Assets folder contains all relevant pictures, images, video and other materials used in the project
- data folder stores all music and play logs files in JSON format.
- Create_s3 is used to create a S3 bucket
- etl.ipynb is the notebook that test the ETL processes
- etl.py reads and processes files from song_data and log_data and loads them into S3 bucket
- README.md provides information on your project


## Run program
- Run Create_s3.py to create a S3 bucket for storing the schema
- Run etl.py to extract, transform and load the data to the S3 bucket


## Database schema design

The schema has one measure table songplays and four dimension table time, user, songs and artists. Please see the ERD below to check the details.

![ERD](/assets/Data Model.png "ERD")


## Maintainer

Allen Chen
email: chenhm03@gmail.com

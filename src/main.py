import logging
import argparse
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pathlib import Path
import shutil
from schemas import calendar_schema, reviews_schema, listings_schema
from transform import transform_listings, transform_calendar, transform_reviews
from utils import (
    write_df_to_csv,
    write_df_to_s3,
    clean_folder,
    rename_files,
    get_dataframes,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write_to_s3", action="store_true", help="flag to write data to s3"
    )
    args = parser.parse_args()
    return args


def get_spark_session(use_s3=False):
    if use_s3:
        return (
            SparkSession.builder.master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
            )
            .config("spark.executor.memory", "16g")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "12")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWSAKEYID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWSSECKEYID"))
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .appName("AirbnbExercises")
            .getOrCreate()
        )

    return (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "16g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "12")
        .appName("AirbnbExercises")
        .getOrCreate()
    )


def main():
    args = get_args()
    spark = get_spark_session(use_s3=args.write_to_s3)
    parent_folder_path = Path.cwd().parents[0]
    df_listings = spark.read.csv(
        str(parent_folder_path / "data/*_listings*.csv.gz"),
        header=True,
        multiLine=True,
        sep=",",
        quote='"',
        escape='"',
        schema=listings_schema,
    ).repartition(12)

    df_calendar = spark.read.csv(
        str(parent_folder_path / "data/*_calendar*.csv.gz"),
        header=True,
        schema=calendar_schema,
    ).repartition(12)

    df_reviews = spark.read.csv(
        str(parent_folder_path / "data/*_reviews*.csv.gz"),
        header=True,
        multiLine=True,
        schema=reviews_schema,
    ).repartition(12)
    df_calendar = transform_calendar(df_calendar).cache()
    df_reviews = transform_reviews(df_reviews).cache()
    df_listings = transform_listings(df_listings, df_calendar).cache()
    dataframes = get_dataframes(
        df_listings=df_listings, df_reviews=df_reviews, df_calendar=df_calendar
    )
    df_calendar.unpersist()
    df_reviews.unpersist()
    df_listings.unpersist()
    data_folder = parent_folder_path / "reports"
    tmp_folder_path = parent_folder_path / "src" / "tmp"

    for file_name, dataframe in dataframes.items():
        if args.write_to_s3:
            logging.info("Writing Files to S3")
            write_df_to_s3(df=dataframe, bucket="pyspark-airbnb", file_name=file_name)
            shutil.rmtree(tmp_folder_path)
        else:
            logging.info("Writing Files Locally (To Disk)")
            write_df_to_csv(df=dataframe, folder=data_folder, file_name=file_name)
            clean_folder(data_folder)
            rename_files(data_folder)


if __name__ == "__main__":
    main()

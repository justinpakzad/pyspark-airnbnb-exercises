import logging
from pyspark.sql import SparkSession
from pathlib import Path
from schemas import calendar_schema, reviews_schema, listings_schema
from transform import transform_listings, transform_calendar, transform_reviews
from utils import write_df_to_csv, clean_folder, rename_files, get_dataframes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    spark = (
        SparkSession.builder.master("local[*]").appName("AirbnbExercises").getOrCreate()
    )
    parent_folder_path = Path.cwd().parents[0]
    df_listings = spark.read.csv(
        str(parent_folder_path / "data/*_listings*.csv.gz"),
        header=True,
        multiLine=True,
        sep=",",
        quote='"',
        escape='"',
        schema=listings_schema,
    ).repartition(16)

    df_calendar = spark.read.csv(
        str(parent_folder_path / "data/*_calendar*.csv.gz"),
        header=True,
        schema=calendar_schema,
    ).repartition(16)

    df_reviews = spark.read.csv(
        str(parent_folder_path / "data/*_reviews*.csv.gz"),
        header=True,
        multiLine=True,
        schema=reviews_schema,
    ).repartition(16)
    df_calendar = transform_calendar(df_calendar).cache()
    df_reviews = transform_reviews(df_reviews).cache()
    df_listings = transform_listings(df_listings, df_calendar).cache()
    dataframes = get_dataframes(
        df_listings=df_listings, df_reviews=df_reviews, df_calendar=df_calendar
    )
    data_folder = parent_folder_path / "reports"
    for file_name, dataframe in dataframes.items():
        write_df_to_csv(df=dataframe, folder=data_folder, file_name=file_name)
    clean_folder(data_folder)
    rename_files(data_folder)


if __name__ == "__main__":
    main()

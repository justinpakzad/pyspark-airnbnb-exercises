import logging
from pyspark.sql import SparkSession
from pathlib import Path
from schemas import calendar_schema, reviews_schema, listings_schema
from transform import transform_listings, transform_calendar, transform_reviews
from utils import write_df_to_csv, clean_folder, rename_files
from analytics import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_dataframes(df_reviews, df_listings, df_calendar):
    dataframes = {
        "top_5_listings_by_avg_price": top_5_listings_by_avg_price_monthly(
            df_listings, df_calendar, df_reviews
        ),
        "occupancy_rate_per_month": occupancy_rate_per_month(df_calendar, df_listings),
        "most_reviewed_listings": most_reviewed_listings(df_reviews, df_listings),
        "top_five_host_avgs": top_five_host_avgs(df_listings, df_calendar),
        "most_active_hosts_per_neighbourhood": most_active_hosts_per_neighbourhood(
            df_listings
        ),
        "monthly_revenue_per_neighbourhood": monthly_revenue_per_neighbourhood(
            df_listings, df_calendar
        ),
        "lowest_occupancy_rate_and_review_scores": lowest_occupancy_rate_and_review_scores(
            df_reviews, df_listings, df_calendar
        ),
        "review_distribution_per_neighbourhood": review_distribution_by_neighbourhood(
            df_listings, df_reviews
        ),
        "neighbourhood_amenity_clusters": neighbourhood_amenity_clusters_and_price(
            df_listings
        ),
        "room_type_seasonal_trends": room_type_price_trends_by_season(
            df_listings, df_calendar
        ),
        "most_diverse_hosts_by_property_type": hosts_with_most_listing_types(
            df_listings
        ),
        "super_hosts_vs_regular_hosts": super_hosts_vs_regular(
            df_listings, df_calendar
        ),
        "host_response_times_stats": host_response_and_rates(
            df_listings, df_calendar, df_reviews
        ),
        "listing_stats_by_price_bin": listings_binned_with_stats(
            df_listings, df_calendar
        ),
    }
    return dataframes


def main():
    spark = (
        SparkSession.builder.appName("LearningSpark")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "32g")
        .getOrCreate()
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

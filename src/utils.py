import logging
import boto3
from botocore.exceptions import ClientError
from analytics import *
import logging


def get_dataframes(df_reviews, df_listings, df_calendar):
    logging.info("Fetching DataFrames...")
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
        "daily_price_changes": daily_price_changes(df_listings, df_calendar),
        "host_verification_performance": host_verification_performance(
            df_listings, df_calendar
        ),
        "host_tier_revenue_trends": host_tier_revenue_trends(df_listings, df_calendar),
        "neighbourhood_group_performance": neighbourhood_group_performance(
            df_listings, df_calendar
        ),
        "property_type_trends": property_type_trends(df_listings, df_calendar),
    }
    logging.info("DataFrames Successfully Fetched")
    return dataframes


def write_df_to_csv(df, folder, file_name):
    folder.mkdir(exist_ok=True)
    try:
        df.coalesce(1).write.csv(f"{folder}/{file_name}", mode="overwrite", header=True)
        logging.info(f"Succesfully wrote {file_name}")
    except Exception as e:
        logging.error(f"Failed to write {file_name}: {e}")


def write_df_to_s3(df, bucket, file_name):
    try:
        df.coalesce(1).write.csv(f"s3a://{bucket}/reports/{file_name}", header=True)
        logging.info(f"Succesfully wrote {file_name}")
    except Exception as e:
        logging.error(f"Failed to write {file_name}: {e}")


def clean_folder(folder):
    for file_path in folder.rglob("*"):
        if file_path.name.startswith("_") or file_path.suffix == ".crc":
            file_path.unlink()


def rename_files(folder):
    for csv_file in folder.rglob("*.csv"):
        folder = "/".join(str(csv_file).split("/")[:-1])
        proper_file_name = f'{"".join(str(csv_file).split("/")[-2])}.csv'
        full_path = f"{folder}/{proper_file_name}"
        csv_file.rename(full_path)

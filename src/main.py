import logging
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f
from pathlib import Path
from pyspark.sql.types import (
    StringType,
    FloatType,
    ArrayType,
)
from schemas import calendar_schema, reviews_schema, listings_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def convert_price_col(df):
    return df.withColumn(
        "price", f.translate(f.col("price"), "$", "").cast(FloatType())
    )


def convert_text_to_bool(df, cols):
    return df.withColumns(
        {
            c: f.when(f.trim(f.col(c)) == f.lit("f"), False).otherwise(True)
            for c in cols
            if c in df.columns
        }
    )


def extract_and_convert_listings(df_listings, bool_cols):
    df_listings = (
        df_listings.withColumns(
            {
                c: f.split(f.trim(f.translate(c, "'[]", "")), ",").cast(
                    ArrayType(StringType())
                )
                for c in ["host_verifications", "amenities"]
            }
        )
        .withColumns(
            {
                "host_city": f.trim(f.element_at(f.split("host_location", ","), 1)),
                "host_state": f.trim(f.element_at(f.split("host_location", ","), 2)),
            }
        )
        .drop("host_location")
    )
    df_listings = convert_text_to_bool(df_listings, bool_cols)
    return df_listings


def convert_na_cols(df, cols):
    return df.withColumns(
        {c: f.when(f.col(c) == "N/A", None).otherwise(f.col(c)) for c in cols}
    )


def clean_listings(df_listings, na_cols):
    df_listings = (
        df_listings.withColumns(
            {
                col: f.trim(f.regexp_replace(col, r'(\r|<br/>|""|")', " "))
                for col in ["description", "neighborhood_overview"]
            }
        )
        .withColumns(
            {
                c: f.regexp_replace(f.col(c), "%", "").cast("integer")
                for c in df_listings.columns
                if c.endswith("rate")
            }
        )
        .drop("neighbourhood")
        .withColumnsRenamed(
            {
                "neighbourhood_cleansed": "neighbourhood",
                "neighbourhood_group_cleansed": "neighbourhood_group",
            }
        )
    )
    df_listings = convert_price_col(df_listings)
    df_listings = convert_na_cols(df_listings, na_cols)
    return df_listings


def get_cols_that_contain(df, contain_str):
    return [
        c
        for c in df.columns
        if df.filter(f.col(c).contains(contain_str)).limit(1).count() > 0
    ]


def compute_median_prices(df_calendar):
    return (
        df_calendar.groupBy("listing_id")
        .agg(f.round(f.median("price"), 2).alias("median_price"))
        .select("listing_id", "median_price")
    )


def coalesce_prices(df_listings, df_calendar, columns=None):
    df_median_prices = compute_median_prices(df_calendar=df_calendar)
    df_listings_filtered = df_listings.select(*[columns] if columns else "*")
    df_listings_filtered = (
        df_listings_filtered.join(
            df_median_prices, df_median_prices.listing_id == df_listings_filtered.id
        )
        .withColumn(
            "price_imputed",
            f.coalesce(df_listings_filtered.price, df_median_prices.median_price),
        )
        .drop(
            "price",
            "median_price",
            "listing_id",
            "adjusted_price",
            "minimum_nights",
            "maximum_nights",
        )
    ).withColumnsRenamed({"price_imputed": "price"})
    return df_listings_filtered


def compute_occupancy_rate(df_calendar, grouping_columns):
    df_occ_rates = df_calendar.groupBy(*grouping_columns).agg(
        f.round(
            (f.sum(f.when(f.col("available") == False, 1).otherwise(0)) / f.count("*"))
            * 100,
            2,
        ).alias("occupancy_rate")
    )
    return df_occ_rates


def average_listings_per_neighbourhood(df_listings):
    """
    Identify the top 5 listings with the highest average price for each month.
    Include a tie-breaking condition where listings with the
    same price are ranked by their total review counts.
    """
    return (
        df_listings.select("neighbourhood", "price")
        .groupBy(f.col("neighbourhood"))
        .agg(
            f.round(f.mean("price"), 2).alias("average_price"),
        )
        .orderBy(f.desc("average_price"))
    )


def occupancy_rate_per_month(df_calendar, df_listings):
    """
    Calculate the occupancy rate for each listing per month
    and identify the top 10 listings with the highest rates
    """
    window = Window.orderBy(f.desc("occupancy_rate")).partitionBy("month")
    df_occ_rate = (
        compute_occupancy_rate(
            df_calendar,
            grouping_columns=[
                "listing_id",
                f.date_format("date", "MMMM").alias("month"),
            ],
        )
        .withColumn("rnk", f.dense_rank().over(window))
        .filter(f.col("rnk") <= 10)
        .drop("rnk")
    )

    df_listings_filtered = df_listings.select(
        "id",
        "host_city",
        "host_state",
        f.col("neighbourhood"),
    )

    df_occ_rate = df_occ_rate.join(
        df_listings_filtered, df_occ_rate.listing_id == df_listings.id
    ).drop("id")
    return df_occ_rate


def most_reviewed_listings(df_reviews, df_listings):
    """
    Find the 10 most reviewed listings for each month.
    Exclude listings with fewer than 20 reviews to avoid outliers.
    """
    window = Window.orderBy(f.desc("review_count")).partitionBy("month")

    df_reviews_filtered = (
        df_reviews.select("listing_id")
        .groupBy("listing_id")
        .count()
        .filter(f.col("count") > 20)
    ).drop("count")

    df_reviews_filtered = df_reviews_filtered.join(
        df_reviews, df_reviews_filtered.listing_id == df_reviews.listing_id
    ).drop(df_reviews.listing_id)
    df_listings_filtered = df_listings.select("id", "neighbourhood", "property_type")
    df_top_listings_review = (
        df_reviews_filtered.groupBy(
            "listing_id", f.date_format("date", "MMMM").alias("month")
        )
        .agg(f.count("id").alias("review_count"))
        .withColumn("rnk", f.dense_rank().over(window))
        .filter(f.col("rnk") <= 10)
        .join(
            df_listings_filtered,
            df_reviews_filtered.listing_id == df_listings_filtered.id,
            how="inner",
        )
        .drop(df_listings.id)
    )
    return df_top_listings_review


def top_five_host_avgs(df_listings, df_calendar):
    """
    Identify the top 5 hosts with the most properties
    and calculate their average price across all listings.
    """
    window = Window.orderBy(f.desc("count")).partitionBy(f.lit(0))
    df_top_5_hosts = (
        df_listings.groupBy("host_id")
        .agg(f.count_distinct("id").alias("count"))
        .withColumn("rnk", f.dense_rank().over(window))
        .filter(f.col("rnk") <= 5)
    ).drop("count")

    df_median_prices = compute_median_prices(df_calendar=df_calendar)
    df_listings_filtered = df_listings.select(
        "host_id", "price", f.col("id").alias("listing_id")
    )
    df_top_5_hosts = (
        (
            df_top_5_hosts.join(
                df_listings_filtered,
                df_top_5_hosts.host_id == df_listings_filtered.host_id,
                how="inner",
            )
            .join(
                df_median_prices,
                df_listings_filtered.listing_id == df_median_prices.listing_id,
                how="inner",
            )
            .drop(df_listings_filtered.host_id)
        )
        .groupBy("host_id")
        .agg(f.round(f.mean("price"), 2).alias("avg_price"))
    )
    return df_top_5_hosts


def most_active_hosts_per_neighbourhood(df_listings):
    """
    Identify the neighborhoods with the most active hosts.
    Include additional details such as the average number
    of properties managed per host and the percentage of
    superhosts in each neighborhood.
    """
    df_neighbourhood = (
        df_listings.groupBy("neighbourhood")
        .agg(
            f.count_distinct("host_id").alias("n_hosts"),
            f.count_distinct("id").alias("n_properties"),
            f.round(
                f.sum(f.when(f.col("host_is_superhost") == True, 1).otherwise(0))
                / f.count_distinct(f.col("host_id"))
                * 100,
                2,
            ).alias("percentage_of_superhosts"),
        )
        .withColumn(
            "average_properties_per_host",
            f.round(f.col("n_properties") / f.col("n_hosts"), 2),
        )
    )
    return df_neighbourhood


def monthly_revenue_per_neighbourhood(df_listings, df_calendar):
    """
    Calculate the total monthly revenue for each neighborhood and
    how much the monthly revenues deviate from the yearly average revenue.
    """
    df_listings_filtered = df_listings.select("id", "neighbourhood")
    df_calendar_neighbourhood = (
        df_calendar.join(
            df_listings_filtered,
            df_calendar.listing_id == df_listings_filtered.id,
            how="inner",
        )
        .drop(df_listings_filtered.id)
        .filter(f.col("available") == False)
        .groupBy(f.date_format("date", "MMMM").alias("month"), "neighbourhood")
        .agg(f.sum("price").alias("total_revenue"))
    )
    df_yearly_variance = df_calendar_neighbourhood.groupBy("neighbourhood").agg(
        f.round(
            ((f.sum("total_revenue") - f.mean("total_revenue")) ** 2) / 12, 2
        ).alias("yearly_variance")
    )

    df_monthly_rev = df_calendar_neighbourhood.join(
        df_yearly_variance,
        df_calendar_neighbourhood.neighbourhood == df_yearly_variance.neighbourhood,
        how="inner",
    ).drop(df_yearly_variance.neighbourhood)

    return df_monthly_rev


def lowest_occupancy_rate_and_review_scores(df_reviews, df_listings, df_calendar):
    """
    Identify the 10 listings with the lowest occupancy rates and
    lowest review scores, but only include listings with at least 10 reviews.
    """
    df_reviews_filtered = (
        df_reviews.groupBy("listing_id").count().filter(f.col("count") >= 10)
    ).drop("count")

    df_listings_filtered = df_listings.select(
        "id", "review_scores_rating", "neighbourhood", "host_id"
    )
    df_occ_rates = compute_occupancy_rate(
        df_calendar=df_calendar, grouping_columns=["listing_id"]
    )

    window_occ = Window.orderBy("occupancy_rate").partitionBy(f.lit(0))
    window_score = Window.orderBy("review_scores_rating").partitionBy(f.lit(0))
    df_lowest_occ_reviews = (
        df_reviews_filtered.join(
            df_listings_filtered,
            df_reviews_filtered.listing_id == df_listings_filtered.id,
        )
        .join(
            df_occ_rates,
            df_reviews_filtered.listing_id == df_occ_rates.listing_id,
            how="inner",
        )
        .drop(df_occ_rates.listing_id, df_listings_filtered.id)
        .withColumns(
            {
                "occupancy_rnk": f.dense_rank().over(window_occ),
                "review_score_rnk": f.dense_rank().over(window_score),
            }
        )
        .withColumn("final_rnk", f.col("occupancy_rnk") + f.col("review_score_rnk"))
        .orderBy("final_rnk")
        .limit(10)
        .drop("final_rnk", "occupancy_rnk", "review_score_rnk")
    )
    return df_lowest_occ_reviews


def review_distribution_by_neighbourhood(df_listings, df_reviews):
    """
    Analyze how reviews are distributed across neighborhoods,
    identifying areas with the most active user feedback.
    """
    window = Window.orderBy(f.desc("n_reviews")).partitionBy(f.lit(0))

    df_listings_filtered = df_listings.select(
        "id", "neighbourhood", "review_scores_rating", "neighbourhood_group"
    )
    df_review_counts = df_reviews.groupBy("listing_id").agg(
        f.count("*").alias("n_reviews")
    )
    df_review_distribution = (
        df_review_counts.join(
            df_listings_filtered,
            df_review_counts.listing_id == df_listings_filtered.id,
            how="inner",
        )
        .drop(df_listings_filtered.id)
        .withColumn("rnk", f.dense_rank().over(window))
    )
    return df_review_distribution


def neighbourhood_amenity_clusters_and_price(df_listings):
    """
    Identify the 10 amenities from the top 100
    most common ones that are most strongly associated with higher prices for listings.
    Analyze the price difference between listings that include each amenity and those that do not.
    """
    df_listings_filtered = df_listings.select("id", "neighbourhood", "price")
    df_avg_amenities = df_listings.groupBy("neighbourhood").agg(
        f.round(f.mean("n_amenities"), 2).alias("avg_amenities")
    )

    _, q2, q3, _ = df_avg_amenities.approxQuantile(
        "avg_amenities", [0.0, 0.25, 0.5, 0.75], 0
    )

    df_amenity_binned = df_avg_amenities.withColumn(
        "avg_amenities_bin",
        f.when(f.col("avg_amenities") <= q2, "Low")
        .when(f.col("avg_amenities").between((q2 + 1), q3), "Medium")
        .otherwise("High"),
    )

    df_amenity_binned = (
        df_listings_filtered.join(
            df_amenity_binned,
            df_listings_filtered.neighbourhood == df_amenity_binned.neighbourhood,
        )
        .drop(df_amenity_binned.neighbourhood)
        .groupBy("avg_amenities_bin")
        .agg(
            f.round(f.mean("price"), 2).alias("avg_price"),
            f.count(f.col("neighbourhood")).alias("n_neighbourhoods"),
        )
    )
    lowest_avg = (
        df_amenity_binned.select("avg_price")
        .filter(f.col("avg_amenities_bin") == f.lit("Low"))
        .collect()[0]
        .avg_price
    )

    df_amenity_binned = df_amenity_binned.withColumn(
        "price_diff_from_low", f.round((f.col("avg_price") - lowest_avg), 2)
    )
    return df_amenity_binned


def room_type_price_trends_by_season(df_listings, df_calendar):
    """
    Analyze how the average prices for different room types vary across the seasons.
    Determine whether certain room types are more affected by seasonal changes than others.
    """
    df_seasons = df_calendar.withColumn(
        "season",
        f.when(f.dayofyear("date").between(79, 171), "Spring")
        .when(f.dayofyear("date").between(172, 263), "Summer")
        .when(f.dayofyear("date").between(264, 354), "Autumn")
        .when(
            (f.dayofyear("date").between(1, 78))
            | (f.dayofyear("date").between(355, 366)),
            "Winter",
        ),
    ).select("listing_id", "date", "available", "price", "season")

    df_listings_filtered = df_listings.select("id", "room_type")

    df_seasons = (
        df_seasons.join(
            df_listings_filtered, df_seasons.listing_id == df_listings_filtered.id
        )
        .drop("id")
        .groupBy("room_type", "season")
        .agg(f.round(f.mean("price"), 2).alias("avg_price"))
    )
    df_seasonal_diffs = (
        df_seasons.groupBy("room_type")
        .agg(
            f.max("avg_price").alias("max_avg_price"),
            f.min("avg_price").alias("min_avg_price"),
        )
        .withColumn(
            "seasonal_price_diff",
            f.round(f.col("max_avg_price") - f.col("min_avg_price"), 2),
        )
        .drop("max_avg_price", "min_avg_price")
    )
    df_seasons = df_seasons.join(
        df_seasonal_diffs, df_seasons.room_type == df_seasonal_diffs.room_type
    ).drop(df_seasonal_diffs.room_type)
    return df_seasons


def hosts_with_most_listing_types(df_listings):
    """
    Identify the top 5 hosts with the
    most diverse types of property listings
    """
    df_diverse_hosts = (
        df_listings.groupBy("host_id")
        .agg(f.count_distinct(f.col("property_type")).alias("n_property_types"))
        .orderBy(f.desc("n_property_types"))
        .limit(5)
        .select("host_id")
    )

    df_listings_filtered = df_listings.select("host_id", "property_type")
    df_top_5_hosts_by_p_types = (
        df_listings_filtered.join(
            df_diverse_hosts, df_diverse_hosts.host_id == df_listings_filtered.host_id
        )
        .drop(df_listings_filtered.host_id)
        .groupBy("host_id", "property_type")
        .agg(f.count("property_type").alias("n_properties"))
    )
    return df_top_5_hosts_by_p_types


def super_hosts_vs_regular(df_listings, df_calendar):
    """
    Compare the average occupancy rates, prices, and review scores
    of superhosts versus non-superhosts.
    """
    df_listings_filtered = df_listings.select(
        "host_id",
        "id",
        "host_is_superhost",
        "review_scores_rating",
        "review_scores_cleanliness",
        "review_scores_communication",
        "review_scores_value",
        "price",
    )

    df_occupancy = compute_occupancy_rate(
        df_calendar=df_calendar, grouping_columns=["listing_id"]
    )
    df_listings_filtered = df_listings_filtered.join(
        df_occupancy, df_listings_filtered.id == df_occupancy.listing_id
    ).drop(df_listings_filtered.id)
    df_super_hosts_agged = df_listings_filtered.groupBy("host_is_superhost").agg(
        f.round(f.mean("review_scores_rating"), 2).alias("avg_review_score_rating"),
        f.round(f.mean("review_scores_cleanliness"), 2).alias(
            "avg_review_score_cleanliness"
        ),
        f.round(f.mean("review_scores_communication"), 2).alias(
            "avg_review_score_communication"
        ),
        f.round(f.mean("review_scores_value"), 2).alias("avg_review_score_value"),
        f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
        f.round(f.mean("price"), 2).alias("avg_price"),
        f.round(f.median("price"), 2).alias("median_price"),
        f.count_distinct(f.col("listing_id").alias("n_listings")),
    )
    return df_super_hosts_agged


def host_response_and_rates(df_listings, df_calendar, df_reviews):
    """
    Analyze how host response times correlate with occupancy rates and reviews.
    """

    df_occ_rate = compute_occupancy_rate(df_calendar, grouping_columns=["listing_id"])

    df_listings_filtered = df_listings.select(
        "id", "host_response_time", "review_scores_rating"
    ).filter(f.col("host_response_time").isNotNull())

    df_host_response_agged = (
        df_listings_filtered.alias("listings")
        .join(
            df_occ_rate.alias("occ_rate"),
            f.col("listings.id") == f.col("occ_rate.listing_id"),
        )
        .drop(f.col("listings.id"))
        .join(
            df_reviews.alias("reviews"),
            f.col("occ_rate.listing_id") == f.col("reviews.listing_id"),
        )
        .drop(f.col("reviews.listing_id"))
        .groupBy("host_response_time")
        .agg(
            f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
            f.round(f.mean("review_scores_rating"), 2).alias("avg_review_score_rating"),
            f.count_distinct("occ_rate.listing_id").alias("n_properties"),
            f.count_distinct("reviews.id").alias("n_reviews"),
        )
    )
    return df_host_response_agged


def listings_binned_with_stats(df_listings, df_calendar):
    """
    Bin listings into price ranges (e.g., Low, Medium, High)
    based on their average prices and
    analyze the occupancy rates and review scores within each bin.
    """
    df_occ_rate = compute_occupancy_rate(
        df_calendar=df_calendar, grouping_columns=["listing_id"]
    )
    df_avg_prices = df_calendar.groupBy("listing_id").agg(
        f.mean("price").alias("avg_price")
    )
    _, q2, q3, _ = df_avg_prices.approxQuantile(
        "avg_price", [0.0, 0.25, 0.5, 0.75], 0.01
    )

    df_avg_prices_binned = df_avg_prices.withColumn(
        "price_bin",
        f.when(f.col("avg_price").between(0, q2), "Low")
        .when(f.col("avg_price").between(q2 + 1, q3), "Medium")
        .otherwise("High"),
    )
    df_review_scores = df_listings.select(
        "id",
        "review_scores_rating",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
    )
    df_avg_prices_binned = df_avg_prices_binned.join(
        df_occ_rate, df_avg_prices_binned.listing_id == df_occ_rate.listing_id
    ).drop(df_occ_rate.listing_id)
    df_avg_prices_binned_with_score = (
        df_avg_prices_binned.join(
            df_review_scores, df_review_scores.id == df_avg_prices_binned.listing_id
        )
        .groupBy("price_bin")
        .agg(
            f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
            f.round(f.mean("review_scores_rating"), 2).alias(
                "average_review_score_rating"
            ),
            f.round(f.mean("review_scores_cleanliness"), 2).alias(
                "average_review_score_cleanliness"
            ),
            f.round(f.mean("review_scores_checkin"), 2).alias(
                "average_review_score_checkin"
            ),
            f.round(f.mean("review_scores_communication"), 2).alias(
                "average_review_score_communication"
            ),
            f.round(f.mean("review_scores_location"), 2).alias(
                "average_review_score_location"
            ),
        )
    )

    return df_avg_prices_binned_with_score


def transform_listings(df_listings, df_calendar):
    bool_cols_to_convert = [
        "has_availability",
        "host_has_profile_pic",
        "host_identity_verified",
        "has_availability",
        "instant_bookable",
    ]

    na_cols = get_cols_that_contain(df_listings, "N/A")
    df_listings = clean_listings(df_listings=df_listings, na_cols=na_cols)
    df_listings = extract_and_convert_listings(
        df_listings=df_listings, bool_cols=bool_cols_to_convert
    ).drop("reviews_per_month")
    df_listings = df_listings.withColumn("n_amenities", f.size(f.col("amenities")))
    df_listings = coalesce_prices(
        df_listings=df_listings, df_calendar=df_calendar, columns=None
    )
    return df_listings


def transform_calendar(df_calendar):
    df_calendar = convert_price_col(df=df_calendar)
    df_calendar = convert_text_to_bool(df=df_calendar, cols=["available"])
    return df_calendar


def transform_reviews(df_reviews):
    df_reviews = (
        df_reviews.withColumns(
            {c: f.col(c).cast("long") for c in df_reviews.columns if "id" in c}
        )
        .withColumn("date", f.to_date("date"))
        .withColumn(
            "comments",
            f.trim(
                f.regexp_replace(
                    f.regexp_replace("comments", r'(\r|<br\s*/?>|""|")', " "),
                    r"\s+",
                    " ",
                )
            ),
        )
    )
    return df_reviews


def write_df_to_csv(df, folder, file_name):
    folder.mkdir(exist_ok=True)
    try:
        df.coalesce(1).write.csv(f"{folder}/{file_name}", mode="overwrite", header=True)
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


def get_dataframes(df_reviews, df_listings, df_calendar):
    dataframes = {
        "avg_listings_per_neighbourhood": average_listings_per_neighbourhood(
            df_listings
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

from pyspark.sql import Window
from pyspark.sql import functions as f
from transform import (
    compute_occupancy_rate,
    compute_median_prices,
    bin_column_by_quantile,
)


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


def top_5_listings_by_avg_price_monthly(df_listings, df_calendar, df_reviews):
    """
    Identify the top 5 listings with the highest average price for each month.
    Include a tie-breaking condition where listings with the
    same price are ranked by their total review counts.
    """
    window = Window.orderBy(f.desc("avg_price"), f.desc("n_reviews")).partitionBy(
        "month"
    )

    df_reviews_count = df_reviews.groupBy(
        "listing_id", f.date_format("date", "MMMM").alias("month")
    ).agg(f.count_distinct("id").alias("n_reviews"))

    df_avg_prices = df_calendar.groupby(
        "listing_id", f.date_format("date", "MMMM").alias("month")
    ).agg(f.mean("price").alias("avg_price"))
    df_listings_filtered = df_listings.select(
        "id",
        "neighbourhood",
    )
    df_listings_rnked = (
        df_avg_prices.join(
            df_reviews_count,
            (df_avg_prices.listing_id == df_reviews_count.listing_id)
            & (df_avg_prices.month == df_reviews_count.month),
            how="left",
        )
        .drop(df_reviews_count.listing_id, df_reviews_count.month)
        .withColumns({c: f.coalesce(c, f.lit(0)) for c in ["avg_price", "n_reviews"]})
        .withColumn("rnk", f.dense_rank().over(window))
        .filter(f.col("rnk") <= 5)
    )

    df_listings_rnked = df_listings_rnked.join(
        df_listings_filtered, df_listings_rnked.listing_id == df_listings_filtered.id
    ).drop(df_listings_filtered.id)

    return df_listings_rnked


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
        .drop(df_reviews_filtered.listing_id, df_listings_filtered.id)
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
        .drop()
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

    df_amenity_binned = bin_column_by_quantile(
        df=df_avg_amenities, column_to_bin="avg_amenities"
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

    df_avg_prices_binned = bin_column_by_quantile(
        df_avg_prices, column_to_bin="avg_price"
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
        .groupBy("avg_price")
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


def daily_price_changes(df_listings, df_calendar):
    """
    Identify listings that actively utilize dynamic pricing strategies.
    For each listing, calculate the percentage of days in the year where the
    price changes compared to the previous day.
    Rank the top 5 listings that exhibit the most frequent price changes.
    """
    window_lag = Window.orderBy("date").partitionBy("listing_id")
    window_rnk = Window.orderBy(f.desc("n_price_changes")).partitionBy(f.lit(0))
    df_listings_filtered = df_listings.select(
        "id", "neighbourhood", "review_scores_rating"
    )
    df_price_changes = (
        df_calendar.withColumn("prev_days_price", f.lag("price").over(window_lag))
        .groupBy("listing_id")
        .agg(
            f.sum(
                f.when(f.col("price") != f.col("prev_days_price"), 1).otherwise(0)
            ).alias("n_price_changes"),
            f.round(
                (
                    f.sum(
                        f.when(f.col("price") != f.col("prev_days_price"), 1).otherwise(
                            0
                        )
                    )
                    / 365
                )
                * 100
            ).alias("pct_of_daily_price_diffs"),
            f.round(f.mean("price"), 2).alias("avg_price"),
        )
        .filter(f.col("n_price_changes") > 0)
        .withColumn("n_price_changes_rnk", f.dense_rank().over(window_rnk))
        .filter(f.col("n_price_changes_rnk") <= 5)
        .drop("n_price_changes_rnk")
    )

    df_price_changes = df_price_changes.join(
        df_listings_filtered, df_price_changes.listing_id == df_listings_filtered.id
    )
    return df_price_changes


def host_verification_performance(df_listings, df_calendar):
    """
    Evaluate how host identity verification status impacts booking performance,
    including proxy booking volume (since we don't directly have booking data),
    occupancy rates,revenue, and review scores for their listings.
    """
    df_listings_filtered = df_listings.select(
        "id",
        "host_identity_verified",
        "review_scores_rating",
        "host_id",
        "neighbourhood",
        "room_type",
    )
    df_hosts_agged = df_listings_filtered.groupBy("host_id").agg(
        f.coalesce(f.round(f.mean("review_scores_rating"), 2), f.lit(0)).alias(
            "avg_review_score_rating"
        ),
        f.max(f.col("host_identity_verified")).alias("host_identity_verified"),
    )
    df_occ_rate = compute_occupancy_rate(
        df_calendar=df_calendar,
        grouping_columns=["listing_id", f.date_format("date", "MMMM").alias("month")],
    )
    df_host_avg_monthly_occ_rate = (
        df_listings_filtered.join(
            df_occ_rate, df_listings_filtered.id == df_occ_rate.listing_id
        )
        .groupBy("host_id")
        .agg(f.round(f.mean("occupancy_rate")).alias("avg_monthly_occ_rate"))
    )

    df_host_avg_monthly_rev = (
        (
            df_calendar.groupBy(
                "listing_id", f.date_format("date", "MMMM").alias("month")
            )
            .agg(
                f.sum(
                    f.when(f.col("available") == False, f.col("price")).otherwise(0)
                ).alias("revenue"),
                f.sum(f.when(f.col("available") == False, 1).otherwise(0)).alias(
                    "n_bookings"
                ),
            )
            .join(
                df_listings_filtered, df_calendar.listing_id == df_listings_filtered.id
            )
        )
        .groupBy("host_id")
        .agg(
            f.round(f.coalesce(f.mean("revenue"), f.lit(0)), 2).alias(
                "avg_monthly_revenue"
            ),
            f.round(f.mean("n_bookings")).alias("average_monthly_bookings"),
        )
    )

    df_hosts_performance = df_host_avg_monthly_occ_rate.join(
        df_host_avg_monthly_rev,
        df_host_avg_monthly_occ_rate.host_id == df_host_avg_monthly_rev.host_id,
    ).drop(df_host_avg_monthly_rev.host_id)

    df_hosts_performance = df_hosts_performance.join(
        df_hosts_agged, df_hosts_performance.host_id == df_hosts_agged.host_id
    ).drop(df_hosts_performance.host_id)

    return df_hosts_performance.select(
        "host_id",
        f.when(f.col("host_identity_verified") == True, "Verified")
        .otherwise("Not Verified")
        .alias("is_verified"),
        "avg_monthly_occ_rate",
        "avg_monthly_revenue",
        "average_monthly_bookings",
        "avg_review_score_rating",
    )


def host_tier_revenue_trends(df_listings, df_calendar):
    """
    Analyze the performance of hosts by categorizing them into
    tiers based on the total number of listings they manage.
    Evaluate how these tiers correlate with total revenue, review scores,
    and listing diversity
    """
    window = Window.orderBy(
        f.desc("total_revenue"), f.desc("n_properties")
    ).partitionBy(f.lit(0))
    df_hosts_binned = (
        df_listings.groupBy("host_id")
        .agg(
            f.count_distinct("id").alias("n_properties"),
            f.coalesce(f.round(f.mean("review_scores_rating"), 2), f.lit(0)).alias(
                "avg_review_score_rating"
            ),
            f.count_distinct("room_type").alias("n_room_types"),
            f.count_distinct("property_type").alias("n_property_types"),
            f.count_distinct("neighbourhood").alias("n_neighbourhoods"),
        )
        .withColumn(
            "n_listings_bin",
            f.when(f.col("n_properties").between(1, 5), "Small")
            .when(f.col("n_properties").between(6, 15), "Medium")
            .otherwise("Large"),
        )
    )

    df_occ_rates = compute_occupancy_rate(df_calendar, ["listing_id"])
    df_revenue = df_calendar.groupBy("listing_id").agg(
        f.sum(f.when(f.col("available") == False, f.col("price")).otherwise(0)).alias(
            "total_revenue"
        )
    )
    df_listings_filtered = df_listings.select("id", "host_id")

    df_host_stats = (
        df_listings_filtered.join(
            df_occ_rates, df_listings_filtered.id == df_occ_rates.listing_id
        )
        .drop(df_occ_rates.listing_id)
        .join(df_revenue, df_listings_filtered.id == df_revenue.listing_id)
        .drop(df_listings_filtered.id)
        .groupBy("host_id")
        .agg(
            f.sum("total_revenue").alias("total_revenue"),
            f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
        )
    )
    df_hosts_binned_with_rnks = (
        df_hosts_binned.join(
            df_host_stats, df_hosts_binned.host_id == df_host_stats.host_id
        )
        .drop(df_host_stats.host_id)
        .withColumn("rnk", f.dense_rank().over(window))
    )

    return df_hosts_binned_with_rnks


def neighbourhood_group_performance(df_listings, df_calendar):
    """
    Rank neighborhood groups by their total revenue contribution and
    analyze how listing diversity (room types and property types) and
    occupancy rates contribute to revenue variations across groups.
    """
    window = Window.orderBy(f.desc("total_revenue")).partitionBy(f.lit(0))
    df_occ_rate = compute_occupancy_rate(df_calendar, grouping_columns=["listing_id"])
    df_revenue = df_calendar.groupBy("listing_id").agg(
        f.sum(f.when(f.col("available") == False, f.col("price")).otherwise(0)).alias(
            "revenue"
        )
    )
    df_rev_occ_rate = df_occ_rate.join(
        df_revenue, df_occ_rate.listing_id == df_revenue.listing_id
    ).drop(df_revenue.listing_id)

    df_listings_filtered = df_listings.select(
        "neighbourhood_group",
        "id",
        "room_type",
        "property_type",
        "review_scores_rating",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
    )
    df_neighbourhoods_agged = (
        df_listings_filtered.join(
            df_rev_occ_rate, df_listings_filtered.id == df_rev_occ_rate.listing_id
        )
        .drop(df_listings_filtered.id)
        .filter(f.col("neighbourhood_group").isNotNull())
        .groupBy("neighbourhood_group")
        .agg(
            f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
            f.sum("revenue").alias("total_revenue"),
            f.round(f.mean("review_scores_rating")).alias("avg_review_score_rating"),
            f.round(f.mean("review_scores_cleanliness")).alias(
                "avg_review_score_cleanliness"
            ),
            f.round(f.mean("review_scores_checkin")).alias("avg_review_score_checkin"),
            f.round(f.mean("review_scores_communication")).alias(
                "avg_review_score_communication"
            ),
            f.count_distinct("room_type").alias("n_unique_room_types"),
            f.count_distinct("property_type").alias("n_unique_property_types"),
        )
        .withColumn("revenue_rank", f.dense_rank().over(window))
    )
    return df_neighbourhoods_agged


def property_type_trends(df_listings, df_calendar):
    window = Window.orderBy(f.desc("price_to_performance_ratio")).partitionBy(f.lit(0))
    df_occ_rate = compute_occupancy_rate(df_calendar, grouping_columns=["listing_id"])
    df_avg_prices = df_calendar.groupBy("listing_id").agg(
        f.round(f.mean("price"), 2).alias("avg_price")
    )
    df_listings_filtered = df_listings.select(
        "property_type", "id", "review_scores_rating"
    )
    df_prices_occ_rate = df_occ_rate.join(
        df_avg_prices, df_occ_rate.listing_id == df_avg_prices.listing_id
    ).drop(df_avg_prices.listing_id)

    df_property_type_agged = (
        df_listings_filtered.join(
            df_prices_occ_rate, df_listings_filtered.id == df_prices_occ_rate.listing_id
        )
        .drop(df_listings_filtered.id)
        .groupBy("property_type")
        .agg(
            f.round(f.mean("review_scores_rating"), 2).alias("avg_review_score_rating"),
            f.round(f.mean("occupancy_rate"), 2).alias("avg_occupancy_rate"),
            f.round(f.mean("avg_price"), 2).alias("avg_price"),
        )
        .withColumn(
            "performance_score",
            f.round(
                (f.col("avg_review_score_rating") * 0.6)
                + (f.col("avg_occupancy_rate") * 0.4),
                2,
            ),
        )
        .withColumn(
            "price_to_performance_ratio",
            f.round(f.col("avg_price") / f.col("performance_score"), 2),
        )
        .withColumn("rank", f.dense_rank().over(window))
    )

    return df_property_type_agged

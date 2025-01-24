from pyspark.sql import functions as f
from pyspark.sql.types import (
    StringType,
    FloatType,
    ArrayType,
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


def bin_column_by_quantile(df, column_to_bin):
    _, q2, q3, _ = df.approxQuantile(column_to_bin, [0.0, 0.25, 0.5, 0.75], 0)
    df_binned = df.withColumn(
        f"{column_to_bin}_bin",
        f.when(f.col(column_to_bin) <= q2, "Low")
        .when(f.col(column_to_bin).between((q2 + 1), q3), "Medium")
        .otherwise("High"),
    )
    return df_binned


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

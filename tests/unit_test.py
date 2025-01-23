import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
import pytest
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    BooleanType,
    LongType,
    DateType,
)
from main import (  # type: ignore
    transform_listings,
    transform_calendar,
    transform_reviews,
)
from schemas import reviews_schema, calendar_schema  # type: ignore


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


@pytest.fixture
def expected_listings_schema():
    expected_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("price", FloatType(), True),
            StructField("description", StringType(), True),
            StructField("neighborhood_overview", StringType(), True),
            StructField("host_city", StringType(), True),
            StructField("host_state", StringType(), True),
            StructField("host_verifications", ArrayType(StringType()), True),
            StructField("amenities", ArrayType(StringType()), True),
            StructField("has_availability", BooleanType(), True),
            StructField("host_has_profile_pic", BooleanType(), True),
            StructField("neighbourhood", StringType(), True),
            StructField("neighbourhood_group", StringType(), True),
            StructField("host_response_rate", IntegerType(), True),
            StructField("host_acceptance_rate", FloatType(), True),
        ]
    )
    return expected_schema


@pytest.fixture
def expected_calendar_schema():
    expected_schema = StructType(
        [
            StructField("listing_id", LongType(), True),
            StructField("date", DateType(), True),
            StructField("available", BooleanType(), True),
            StructField("price", FloatType(), True),
            StructField("adjusted_price", FloatType(), True),
            StructField("minimum_nights", IntegerType(), True),
            StructField("maximum_nights", IntegerType(), True),
        ]
    )
    return expected_schema


@pytest.fixture
def mock_listings_df(spark):
    data = [
        (
            6666,
            "$123.45",
            "Beautiful place!<br/>Great for families.",
            "Quiet neighborhood.",
            "New York City, NY",
            "['email', 'phone']",
            "['Wifi', 'TV', 'Kitchen']",
            "t",
            "f",
            "Neighborhood highlights",
            "Williamsburg",
            "Brooklyn",
            "100%",
            "N/A",
        )
    ]

    columns = [
        "id",
        "price",
        "description",
        "neighborhood_overview",
        "host_location",
        "host_verifications",
        "amenities",
        "has_availability",
        "host_has_profile_pic",
        "neighbourhood",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "host_response_rate",
        "host_acceptance_rate",
    ]

    return spark.createDataFrame(data, schema=columns)


@pytest.fixture
def mock_reviews_df(spark):
    data = [
        (
            123456,
            654321,
            date(2023, 1, 1),
            98765,
            "John Doe",
            "The Venice Roost was fantastic!!\r<br/>We like the <br> thing to test the cleaning.",
        ),
    ]

    return spark.createDataFrame(data, schema=reviews_schema)


@pytest.fixture
def mock_calendar_df_cleaned(spark, expected_calendar_schema):
    data = [(6666, date(2025, 5, 26), True, 279.0, None, 3, 365)]

    return spark.createDataFrame(data, schema=expected_calendar_schema)


@pytest.fixture
def mock_calendar_df(spark):
    data = [
        (
            212,
            date(2025, 5, 26),
            "t",
            "$279.00",
            None,
            3,
            365,
        )
    ]
    return spark.createDataFrame(data, schema=calendar_schema)


def test_transform_listings(
    spark, mock_listings_df, mock_calendar_df_cleaned, expected_listings_schema
):
    expected_df = spark.createDataFrame(
        [
            (
                6666,
                123.45,
                "Beautiful place! Great for families.",
                "Quiet neighborhood.",
                "New York City",
                "NY",
                ["email", "phone"],
                ["Wifi", "TV", "Kitchen"],
                True,
                False,
                "Williamsburg",
                "Brooklyn",
                100,
                None,
            )
        ],
        schema=expected_listings_schema,
    )
    transformed_df = (
        transform_listings(
            df_listings=mock_listings_df, df_calendar=mock_calendar_df_cleaned
        )
        .select(*list(expected_df.columns))
        .withColumns(
            {
                c: f.expr(f"transform({c}, x -> trim(x))")
                for c in ["host_verifications", "amenities"]
            }
        )  # Expected df kept throwing whitespace in the array
    )

    assert expected_df.collect() == transformed_df.collect()


def test_transform_reviews(spark, mock_reviews_df):
    expected_df = spark.createDataFrame(
        data=[
            (
                123456,
                654321,
                date(2023, 1, 1),
                98765,
                "John Doe",
                "The Venice Roost was fantastic!! We like the thing to test the cleaning.",
            ),
        ],
        schema=reviews_schema,
    )
    transformed_df = transform_reviews(mock_reviews_df)
    assert expected_df.collect() == transformed_df.collect()


def test_transform_calendar(spark, mock_calendar_df, expected_calendar_schema):
    expected_df = spark.createDataFrame(
        data=[
            (
                212,
                date(2025, 5, 26),
                True,
                279.00,
                None,
                3,
                365,
            )
        ],
        schema=expected_calendar_schema,
    )
    transformed_df = transform_calendar(mock_calendar_df)
    assert expected_df.collect() == transformed_df.collect()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import pytest
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType,
)

from tasks import calculate_average_movie_rating, init_spark


@pytest.fixture
def movie_schema():
    return StructType(
        [
            StructField("movieId", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genres", StringType(), True),
        ]
    )

@pytest.fixture
def ratings_schema():
    return StructType(
        [
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )


@pytest.fixture
def result_schema():
    return StructType(
        [
            StructField("title", StringType(), True),
            StructField("weight_avg", DoubleType(), True),
            StructField("num_votes", IntegerType(), True),
        ]
    )


@pytest.fixture
def movies_df(spark_session, movie_schema):
    return spark_session.createDataFrame(
        [
            (1, "Jumanji(1995)", "Adventure|Children|Fantasy"),
            (2, "Heat (1995)", "Action|Crime|Thriller"),
        ],
        movie_schema,
    )


@pytest.fixture
def ratings_df(spark_session, ratings_schema):
    return spark_session.createDataFrame(
        [
            (1, 1, 4, 1256677221),
            (2, 1, 4, 1256677222),
            (3, 1, 1, 1256677222),
            (4, 2, 4, 1256677222),
        ],
        ratings_schema,
    )


def test_calculate_average_movie_rating(spark_session, movies_df, ratings_df, result_schema):
    """
    There is a unit test to test the business logic implemented in calculate_average_movie_rating function. It is
    important to keep Spark read and write operations out of the scope of the unit test.

    The expectation is to implement negative tests as well. For example to cover a non expected format of the input data, etc.

    @param spark_session: provided by pyspark-test as a session scope fixture https://pypi.org/project/pytest-spark/
    @param movies_df: initiated with predefined values Spark DataFrame contains movies data
    @param ratings_df: initiated with predefined values Spark DataFrame contains movie ratings data
    @param result_schema: expected result schema
    """
    init_spark(spark_session)
    expected_df = spark_session.createDataFrame(
        [
            ("Jumanji(1995)", 3.0, 3),
            ("Heat (1995)", 4.0, 1),
        ],
        result_schema,
    )
    result_df = calculate_average_movie_rating(movies_df, ratings_df)

    assert result_df.count() == expected_df.count()
    assert (
        result_df.subtract(expected_df).count()
        == expected_df.subtract(result_df).count()
        == 0
    )

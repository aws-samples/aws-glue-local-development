# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import pytest
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import col
from tasks import calculate_top10_movies, get_spark, init_spark


@pytest.fixture
def input_schema():
    return StructType(
        [
            StructField("title", StringType(), True),
            StructField("weight_avg", DoubleType(), True),
            StructField("num_votes", IntegerType(), True),
        ]
    )


@pytest.fixture
def result_schema():
    return StructType(
        [
            StructField("title", StringType(), True),
            StructField("sum_avg", DoubleType(), True),
        ]
    )


@pytest.fixture
def ratings_df(spark_session, input_schema):
    return spark_session.createDataFrame(
        [
            ("TestMovie1", 1.0, 3),
            ("TestMovie2", 2.0, 1),
            ("TestMovie3", 3.0, 1),
            ("TestMovie4", 4.0, 1),
            ("TestMovie5", 5.0, 1),
            ("TestMovie6", 6.0, 1),
            ("TestMovie7", 7.0, 1),
            ("TestMovie8", 8.0, 1),
            ("TestMovie9", 9.0, 1),
            ("TestMovie10", 10.0, 1),
            ("TestMovie11", 11.0, 1),
        ],
        input_schema,
    )


def test_calculate_top10_movies(spark_session, ratings_df, result_schema):
    """
    There is a unit test to test the business logic implemented in calculate_top10_movies function. It is
    important to keep Spark read and write operations out of the scope of the unit test.

    The expectation is to implement negative tests as well. For example to cover a non expected format of the input data, etc.

    @param spark_session: provided by pyspark-test as a session scope fixture https://pypi.org/project/pytest-spark/
    @param ratings_df: initiated with predefined values Spark DataFrame contains movie ratings data
    @param result_schema: expected result schema
    """
    init_spark(spark_session)

    expected_df = get_spark().createDataFrame(
        [
            ("TestMovie2", 2.0),
            ("TestMovie3", 3.0),
            ("TestMovie4", 4.0),
            ("TestMovie5", 5.0),
            ("TestMovie6", 6.0),
            ("TestMovie7", 7.0),
            ("TestMovie8", 8.0),
            ("TestMovie9", 9.0),
            ("TestMovie10", 10.0),
            ("TestMovie11", 11.0),
        ],
        result_schema,
    )

    result_df = calculate_top10_movies(ratings_df)

    assert result_df.count() == expected_df.count()
    assert result_df.collect() == expected_df.orderBy(col("sum_avg").desc()).collect()

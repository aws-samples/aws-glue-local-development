# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pyspark.sql import DataFrame
import tasks


def calculate_average_movie_rating(movie_df, ratings_df: DataFrame) -> DataFrame:
    """
    This is a business logic of movie ratings calculation. This function is covered by the unit test.

    @param movie_df: Spark DataFrame contains movies input data
    @param ratings_df: Spark DataFrame contains movie ratings input data
    @return: weighted average of the movie rating
    """
    joined_df = ratings_df.join(movie_df, movie_df.movieId == ratings_df.movieId)

    joined_df.createOrReplaceTempView("movies_ratings")
    result_df = tasks.get_spark().sql(
        """SELECT title, 
            SUM(rating) / COUNT(*) as weight_avg, COUNT(*) as num_votes 
            FROM movies_ratings 
            GROUP BY title 
            ORDER BY num_votes DESC"""
    )

    return result_df


def calculate_top10_movies(average_rating_df: DataFrame) -> DataFrame:
    """
    This is a business logic of top 10 movies calculation. This function is covered by the unit test.

    @param average_rating_df: Spark DataFrame contains the weighted average of the movie rating
    @return: top 10 movies
    """
    average_rating_df.createOrReplaceTempView("movies_average_ratings")
    result_df = tasks.get_spark().sql(
        """SELECT title, 
        SUM(weight_avg) as sum_avg 
        FROM movies_average_ratings 
        GROUP BY title 
        ORDER BY sum_avg desc LIMIT 10"""
    )

    return result_df


def create_and_stage_average_rating(input_path: str, output_path: str, conn_type: str):
    """
    There are multiple layers of the code in order to separate the business logic from Spark read/write operation and
    be able to unit test it fully isolated. This function will read the input data, create Spark DataFrames,
    invoke a business logic of calculate_average_movie_rating and write results.

    @param input_path: input path contains input files to the job
    @param output_path: output path used to write Glue job results
    @param conn_type: defines the file system to read input files and write results
    """
    movie_df = tasks.read_csv_glue(conn_type, f"{input_path}/movies.csv")
    ratings_df = tasks.read_csv_glue(conn_type, f"{input_path}/ratings.csv")

    result_df = calculate_average_movie_rating(movie_df, ratings_df)

    tasks.write_csv_glue(result_df, conn_type, f"{output_path}")


def create_and_stage_top_movies(input_path: str, output_path: str, conn_type: str):
    """
    This function will read the input data, create Spark DataFrames, invoke a business logic of
    calculate_top10_movies and write results.

    @param input_path: input path contains input files to the job
    @param output_path: output path used to write Glue job results
    @param conn_type: defines the file system to read input files and write results
    """
    average_df = tasks.read_csv_glue(conn_type, f"{input_path}")

    result_df = calculate_top10_movies(average_df)

    tasks.write_csv_glue(result_df, conn_type, f"{output_path}")

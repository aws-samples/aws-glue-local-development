# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

# SPARK variable is defined as Global during the initialisation phase to avoid multiple instances of SparkSession
# initialization
SPARK: SparkSession = None

# GLUE variable is defined as Global during the initialisation phase to avoid multiple instances of GlueContext
# initialization
GLUE: GlueContext = None


def init_glue(glue_context):
    """
    Init function of GlueContext, assign an instance of GlueContext to a global variable.

    @param glue_context: GlueContext instance
    """
    if not glue_context:
        raise RuntimeError("Glue context cannot be null")
    global GLUE
    if GLUE is None:
        GLUE = glue_context


def init_spark(spark_session):
    """
    Init function of SparkSession, assign an instance of SparkSession to a global variable.

    @param spark_session: SparkSession instance
    """
    if not spark_session:
        raise RuntimeError("Spark Session cannot be null")
    global SPARK
    if SPARK is None:
        SPARK = spark_session


def get_spark():
    return SPARK


def write_csv_glue(df: DataFrame, conn_type: str, path: str) -> None:
    """
    Help function to write a DataFrame to a file system according to the parameters conn_type and path.

    @param df: DataFrame to be written as a Glue job output
    @param conn_type: defines the file system to read input files and write results (file or s3)
    @param path: write path of Glue job results
    """
    dynamic_df = DynamicFrame.fromDF(df, GLUE, "dynamic_df")
    GLUE.write_dynamic_frame.from_options(
        frame=dynamic_df,
        connection_type=conn_type,
        connection_options={"path": path},
        format="csv",
        transformation_ctx="datasink2",
    )


def read_csv_glue(conn_type: str, path: str) -> DataFrame:
    """
    Help function to read an input data as Spark DataFrame according to the parameter conn_type and path.

    @param conn_type: defines the file system to read input files and write results (file or s3)
    @param path: the location of the files to read
    @return: input data as Spark DataFrame
    """
    return (
        GLUE.create_dynamic_frame_from_options(
            connection_type=conn_type,
            format="csv",
            connection_options={"paths": [path]},
            format_options={"withHeader": True, "separator": ","},
        )
    ).toDF()

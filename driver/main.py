# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import importlib

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def create_glue_context(config=None):  # pragma: no cover
    """Build Spark and GlueContext"""

    sc = SparkContext()
    glue_context = GlueContext(sc)

    return glue_context


def main(
    input_path, output_path, conn_type, module_name, function_name, glue_config=None
):
    """
    There is a main function of Glue job driver. The idea is to keep it generic to be able to reuse for running
    multiple Glue jobs. Driver is responsible to validate input parameters and invoke a proper Glue job. It does not
    contain any specific business logic.

    @param input_path: input path contains input files to the job
    @param output_path: output path used to write Glue job results
    @param conn_type: use 'file' to read files from a local file system or 's3' for reading files from S3
    @param module_name and function_name: are used to call Glue job using reflection
    @param glue_config: optional additional Glue configuration parameters

    Parameters example: --JOB_NAME=movie_analytics_job --CONN_TYPE=file --INPUT_PATH=/input --OUTPUT_PATH=/results --MODULE_NAME=tasks --FUNCTION_NAME=create_and_stage_average_rating
    """
    try:
        glue_context = create_glue_context()

        module = importlib.import_module(module_name)
        module.init_glue(glue_context)
        module.init_spark(glue_context.spark_session)
        f = getattr(module, function_name)
        f(input_path, output_path, conn_type)

        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)

        job.commit()

    except Exception as e:
        sys.error(
            f"Couldn't execute job due to {str(e)} for module: {module_name} and function: {function_name}"
        )
        raise e


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "CONN_TYPE",
            "INPUT_PATH",
            "OUTPUT_PATH",
            "MODULE_NAME",
            "FUNCTION_NAME",
        ],
    )

    main(
        input_path=args["INPUT_PATH"],
        output_path=args["OUTPUT_PATH"],
        conn_type=args["CONN_TYPE"],
        module_name=args["MODULE_NAME"],
        function_name=args["FUNCTION_NAME"],
    )

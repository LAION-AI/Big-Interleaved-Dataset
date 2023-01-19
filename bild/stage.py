import logging
import tempfile
import time

import boto3
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from .downtools import download_http, downls_s3
from .pipeline_utils import pipeline
from .spark_session_builder import build_spark_session


def path_config():
    config = dict()
    config["cwd"] = Path().absolute()
    config["rwd"] = config["cwd"] / "raw_store/"
    config["Extraction_store"] = config["rwd"] / "exstore/"
    config["log_store"] = config["rwd"] / "log_store/"
    config["Stats_store"] = config["rwd"] / "stats_store/"
    config["Vidstore"] = config["rwd"] / "vid_store/"
    config["Imgstore"] = config["rwd"] / "img_store/"
    config["Audstore"] = config["rwd"] / "aud_store/"
    config["Iframestore"] = config["rwd"] / "iframe_store/"
    for y in config.values():
        y.mkdir(parents=True, exist_ok=True)
    return config


# def engine(wurl):
#     wfobj = downls_s3(wurl)
#     pipeline(wfobj,wurl,pconfig)
#     return 1


def disengine(pconfig):
    def engine(wurl):
        wfobj = downls_s3(wurl)
        pipeline(wfobj, wurl, pconfig)
        return 1

    return engine


def extractor(amount: int, spark):
    spark = spark
    pqpath = "../assets/sept22.parquet"
    pconfig = path_config()
    df = spark.read.parquet(pqpath).limit(amount)
    df.show()
    engine = disengine(pconfig=pconfig)
    udf_myFunction = udf(engine, IntegerType())
    print("hello")
    df = df.withColumn("Parsed", udf_myFunction("url"))
    df.show()
    print("hello")

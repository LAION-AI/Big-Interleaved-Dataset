import pandas as pd
from pyspark.sql import SparkSession
import boto3
from pathlib import Path
import tempfile
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import logging
import time
from .downtools import downls_s3,download_http
from .spark_session_builder import build_spark_session
from .pipeline_utils import pipeline



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
        pipeline(wfobj,wurl,pconfig)
        return 1
    return engine
    

# pqpath = "./sept22.parquet"


def extractor(amount:int,spark):
    spark = spark
    pqpath = "/fsx/home-harrysaini/Big-Interleaved-Dataset/bild/sept22.parquet"
    pconfig = path_config()
    df=spark.read.parquet(pqpath).limit(amount)
    df.show()
    engine=disengine(pconfig=pconfig)
    udf_myFunction = udf(engine, IntegerType())
    print("hello")
    df = df.withColumn("Parsed", udf_myFunction("url"))
    df.show()
    print("hello")


        

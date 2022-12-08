import pandas as pd
from pyspark.sql import SparkSession
from bild import *
import boto3
from pathlib import Path
import tempfile
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import logging
import time
from bild.spark_session_builder import build_spark_session

def path_config():
    config = dict()
    config["cwd"] = Path().absolute()
    config["Extraction_store"] = config["cwd"] / "exstore/"
    config["log_store"] = config["cwd"] / "log_store/"
    config["Stats_store"] = config["cwd"] / "stats_store/"
    config["Vidstore"] = config["cwd"] / "vid_store/"
    config["Imgstore"] = config["cwd"] / "img_store/"
    config["Audstore"] = config["cwd"] / "aud_store/"
    config["Iframestore"] = config["cwd"] / "iframe_store/"
    for y in config.values():
        y.mkdir(parents=True, exist_ok=True)
    return config


def downls_s3(wurl):
    s3client = boto3.client('s3', use_ssl=False)
    data = tempfile.TemporaryFile()
    s3client.download_fileobj(
    'commoncrawl',
    wurl,
    data
    )
    data.seek(0)
    return data


def downls_http(wurl):
    data = tempfile.TemporaryFile()
    data.seek(0)
    return data

def framer(spark:SparkSession,pqpath,amount):
    df=spark.read.parquet(pqpath).limit(amount)
    return df

def engine(wurl):
    wfobj = downls_s3(wurl)
    #We should read from another point but setting up here
    config=path_config()
    pipeline(wfobj,wurl,config)



 # Let the function returns an int


logging.basicConfig(
    filename="./main.log",
    level=logging.INFO,
    filemode="w",
    format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
pqpath = "/fsx/home-harrysaini/projects/Big-Interleaved-Dataset/bild/sept22.parquet"

def main():
    spark=build_spark_session(master="local",num_cores=16,mem_gb=16)
    st=time.time()
    df=framer(spark,pqpath,1)
    udf_myFunction = udf(engine, IntegerType())
    df = df.withColumn("message", udf_myFunction("url"))
    df.show()
    logging.info(f"This took {time.time()-st}s")

        
if __name__=="__main__":
        main()
   
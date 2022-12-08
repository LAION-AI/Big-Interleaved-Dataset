import pandas as pd
from pyspark.sql import SparkSession
from bild import *
import boto3
from pathlib import Path
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import logging
import time
from bild.downtools import downls_s3,download_http
from bild.spark_session_builder import build_spark_session

# path configuration for the project, used in extraction 
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


# read a dataframe from a parquet file of WARC urls of a given amount P2
def framer(spark:SparkSession,pqpath,amount):
    df=spark.read.parquet(pqpath).limit(amount)
    return df

# the engine that will be applied to each row of the dataframe P2
def engine(wurl):
    wfobj = downls_s3(wurl)
    config=path_config()
    pipeline(wfobj,wurl,config)


logging.basicConfig(
    filename="./main.log",
    level=logging.INFO,
    filemode="w",
    format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)

# path to the parquet file of Warc urls right now sept 22 crawl P2
pqpath = "./bild/sept22.parquet"

def main():
    # build a spark session
    spark=build_spark_session(master="local",num_cores=16,mem_gb=16)

    st=time.time()
    # read the dataframe P2
    df=framer(spark,pqpath,1) # notice we are only reading 1 warc that is the first one
    udf_myFunction = udf(engine, IntegerType())
    df = df.withColumn("message", udf_myFunction("url"))
    df.show()
    logging.info(f"This took {time.time()-st}s")

        
if __name__=="__main__":
        main()
   
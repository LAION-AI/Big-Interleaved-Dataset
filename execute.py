import pandas as pd
from pyspark.sql import SparkSession
from bild import *
import boto3
from pathlib import Path
pqpath = "/fsx/home-harrysaini/Big-Interleaved-Dataset/bild/sept22.parquet"

config = dict()
config["cwd"] = Path().absolute()
config["Extraction_store"] = config["cwd"] / "exstore/"
config["log_store"] = config["cwd"] / "log_store/"
config["Stats_store"] = config["cwd"] / "stats_store/"
for y in config.values():
    y.mkdir(parents=True, exist_ok=True)

import tempfile
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

import tempfile
def downls(wurl):
    data = tempfile.TemporaryFile()
    data.seek(0)
    return data

def framer(spark:SparkSession,pqpath,amount):
    df=spark.read.parquet(pqpath).limit(amount)
    return df



def engine(wurl):
    wfobj = downls_s3(wurl)
    #We should read from another point but setting up here
    pipeline(wfobj,wurl,config)


from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

 # Let the function returns an int

import logging
logging.basicConfig(
    filename="./main.log",
    level=logging.INFO,
    filemode="w",
    format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
import time
spark=SparkSession.builder.getOrCreate()
st=time.time()
df=framer(spark,pqpath,100)
udf_myFunction = udf(engine, IntegerType())
df = df.withColumn("message", udf_myFunction("url"))
df.show()
logging.info(f"This took {time.time()-st}s")
   
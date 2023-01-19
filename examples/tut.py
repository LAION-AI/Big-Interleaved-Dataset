import logging

# from bild import stage.extraction
from bild.spark_session_builder import build_spark_session

# path_config = execute.path_config()
spark = build_spark_session(
    master="spark://cpu128-dy-r6i-32xlarge-49:7077", num_cores=128, mem_gb=256
)

logging.basicConfig(
    filename="./main.log",
    level=logging.INFO,
    filemode="w",
    format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)

if __name__ == "__main__":
    pass
    # extraction(1000, configp=path_config)

import bild.stage as stage
# from bild import build_spark_session
# from argparse import parse
#spark=stage.build_spark_session(master="local",num_cores=16,mem_gb=16)
spark=stage.build_spark_session(master="spark://cpu128-dy-r6i-32xlarge-6:7077",num_cores=128,mem_gb=256)

if __name__ == "__main__":
    stage.extractor(10,spark)



import os
from pyspark.sql import SparkSession
from app import main2

os.environ['PYSPARK_PYTHON'] = "./.venv/bin/python"
spark = SparkSession.builder.config(
    "spark.archives",
    "pyspark_venv.tar.gz#environment").getOrCreate()
main2.start(spark)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("ReadJSONExample").getOrCreate()

schema = StructType([
      StructField("student_name", StringType(), True),
      StructField("score", DoubleType(), True),
      StructField("subject", StringType(), True),
      StructField("date", StringType(), True)
  ])

df = spark.read.option('multiline', True).schema(schema).json("data/exams.json")
df.show()
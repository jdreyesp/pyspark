import pandas as pd
import numpy as np
from pydantic import BaseModel, ValidationError, BaseSettings
from pyspark.sql.functions import pandas_udf, col, mean
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from dataclasses import dataclass

@dataclass
class Score:
    id: int
    v: float

class ScorePydantic(BaseModel):
    id: int
    v: float

class Settings(BaseSettings):
    app_name: str
    debug: bool = False

    class Config:
        env_prefix = 'MYAPP_'

settings = Settings(app_name="MyApp")
print(settings)

def start(spark):
    # dataclass does not implement type validation. Use pydantic instead
    #df = spark.createDataFrame([Score(1, 1.0), Score(1, "hello"), Score(2, 3.0), Score(2, 5.0), Score(2, 10.0)])
    try: 
        df = spark.createDataFrame([ScorePydantic(id=1, v=1.0), ScorePydantic(id=1, v=2.0), ScorePydantic(id=2, v=3.0), ScorePydantic(id=2, v=5.0), ScorePydantic(id=2, v=10.0)])

        #df = spark.createDataFrame(
        #    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        #    ("id", "v"))

        #top_rated = df[df["v"] > 3.0]
        #print(df.withColumnRenamed("v", "score").collect())
        #df.withColumn("double_v", col("v") * 2).show()
        #df.coalesce(1).write.csv("cleaned_reviews.csv", header=True, mode="overwrite")

        @pandas_udf(StringType())
        def has_passed_udf(score_series: pd.Series) -> pd.Series:
            # Inefficient
            #return score_series.apply(lambda score: "Passed" if score > 5.0 else "Not passed")

            # Using NumPy vectorized operation
            return pd.Series(np.where(score_series > 5.0, "Passed", "Not passed"))

        #new_df = df.withColumn("result", has_passed_udf(df["v"]))
        #new_df["v", "result"].show()
        #df.filter(df["v"] > 5.0).show()

        #df.groupBy("id").pivot("v").agg(mean("v")).show()

        spark.createDataFrame(pd.merge(df.toPandas(), df.toPandas(), on='id')).show()
    except ValidationError as e:
        print(e)
if __name__ == "__main__":
    start(SparkSession.builder.getOrCreate())
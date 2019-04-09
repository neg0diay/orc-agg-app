from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum as _sum, desc

# conf = SparkConf().setAppName('PointsAggregatorStandaloneApp')
# sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("PointsAggregatorStandaloneApp").getOrCreate()


def getGroupedPointOwners():
    points_df = spark.read.format("orc").load("./data/*.orc")
    points_df = points_df.withColumn("qty", points_df["qty"].cast(DoubleType()))
    print(points_df.printSchema)
    points_df.show(10)
    points_stats = points_df \
        .groupBy("customerid") \
        .agg(_sum("qty").alias("total qty")).orderBy(desc("total qty"))
    points_stats.show(10)

    return points_stats


if __name__ == '__main__':
    points_stats = getGroupedPointOwners()

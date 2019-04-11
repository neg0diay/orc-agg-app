from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, DateType, TimestampType
from pyspark.sql.functions import sum as _sum, desc, to_date, from_utc_timestamp, from_unixtime
import argparse

# conf = SparkConf().setAppName('PointsAggregatorStandaloneApp')
# sc = SparkContext(conf=conf)

environment = ""
files_directory = ""

spark = SparkSession \
    .builder \
    .appName("PointsAggregatorStandaloneApp").getOrCreate()


def parseInputArguments():
    global files_directory, environment
    parser = argparse.ArgumentParser(description='group points')
    parser.add_argument('--directory', dest='directory',
                        default="./data/*.orc",
                        help='full path to orc files')
    parser.add_argument('--environment', dest='environment',
                        default="local",
                        help='name of environment(local or cluster)')
    args = parser.parse_args()
    files_directory = args.directory
    environment = args.environment
    print("Current file directory: {}".format(files_directory))
    print("Current environment: {}".format(environment))


def getGroupedPointOwners():
    if environment == "local":
        pass
    elif environment == "cluster":
        pass
    else:
        raise AssertionError(
            "Bad environment variable (environment = {}). Should be local or cluster".format(environment))

    points_df = spark.read.format("orc").load(files_directory)

    # df modification
    points_df = points_df.withColumn("qty", points_df["qty"].cast(DoubleType()))
    points_df = points_df.withColumn("period_full_date",
                                     from_unixtime(points_df["period"] / 1000, 'yyyy-MM-dd hh:mm:ss'))
    points_df = points_df.withColumn("period_year_month", from_unixtime(points_df["period"] / 1000, 'yyyy-MM'))

    print(points_df.printSchema)
    points_df.show(10)
    # .groupBy(["period_year_month", "customerid"]) \
    points_stats = points_df \
        .groupBy(["period_year_month", "organisationid", "customerid", "typeid"]) \
        .agg(_sum("qty").alias("total_qty")).orderBy(desc("period_year_month"))

    points_stats.show(100)

    return points_stats


if __name__ == '__main__':
    # pass
    parseInputArguments()
    points_stats = getGroupedPointOwners()

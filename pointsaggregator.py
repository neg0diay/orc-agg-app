from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType, DateType, TimestampType
from pyspark.sql.functions import sum as _sum, desc, to_date, from_utc_timestamp, from_unixtime, to_timestamp, trunc, add_months
import argparse

# conf = SparkConf().setAppName('PointsAggregatorStandaloneApp')
# sc = SparkContext(conf=conf)

environment = ""
files_directory = ""
files_out_directory = ""
files_out_filename = ""

spark = SparkSession \
    .builder \
    .appName("PointsAggregatorStandaloneApp").getOrCreate()


def parseInputArguments():
    global files_directory, environment, files_out_directory, files_out_filename
    parser = argparse.ArgumentParser(description='group points')
    parser.add_argument('--directory', dest='directory',
                        default="./data/*.orc",
                        help='full path to orc files')
    parser.add_argument('--out-directory', dest='out_directory',
                        default="./out/*.orc",
                        help='full path to out orc files')
    parser.add_argument('--out-filename', dest='out_filename',
                        default="my_file.orc",
                        help='full path to out orc files')
    parser.add_argument('--environment', dest='environment',
                        default="local",
                        help='name of environment(local or cluster)')
    args = parser.parse_args()
    files_directory = args.directory
    environment = args.environment
    files_out_directory = args.out_directory
    files_out_filename = args.out_filename
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
    # points_df = points_df.withColumn("qty", points_df["qty"].cast(DoubleType()))
    # points_df = points_df.withColumn("period_full_date",
    #                                  from_unixtime(points_df["period"] / 1000, 'yyyy-MM-dd hh:mm:ss'))
    points_df = points_df.withColumn("period", from_unixtime(points_df["period"] / 1000, 'yyyy-MM-dd hh:mm:ss'))
    #
    # print(points_df.printSchema)
    # points_df.show(10)
    #
    # points_stats = points_df \
    #     .groupBy(["period_year_month", "organisationid", "customerid", "typeid"]) \
    #     .agg(_sum("qty").alias("total_qty")).orderBy(desc("period_year_month"))

    points_df = points_df.withColumn("qty", points_df["qty"].cast(DoubleType()))
    points_df = points_df.withColumn('month', trunc(points_df['period'], 'MM'))

    points_df = points_df.groupby(['organisationid', 'customerid', 'typeid', 'month']).sum('qty')

    points_df = points_df.withColumn("cumulativeSum",
                                     _sum('sum(qty)').over(
                                         Window.partitionBy(['organisationid', 'customerid', 'typeid']).orderBy(
                                             'month')))

    points_df = points_df.withColumn('aggdate', add_months(points_df['month'], 1))
    points_df = points_df.withColumn('aggdate_ts', to_timestamp(points_df['aggdate']))
    points_df = points_df.withColumn('aggdate_date', points_df['month'].cast(DateType()))
    points_df = points_df.withColumn("qty", points_df["cumulativeSum"])

    points_df = points_df.drop('cumulativeSum')
    points_df = points_df.drop('sum(qty)')
    points_df = points_df.drop('month')

    points_df.show(100)

    return points_df


if __name__ == '__main__':
    # pass
    parseInputArguments()
    points_stats = getGroupedPointOwners()
    points_stats.write.mode('overwrite').format("orc").save(files_out_directory + '/' + files_out_filename)

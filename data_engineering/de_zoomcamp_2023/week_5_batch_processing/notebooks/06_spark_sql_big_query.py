
import pyspark
import pandas as pd
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import argparse


parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-19814835343-zbujt5af')

# df_yellow = spark.read.parquet('../code/data/pq/yellow/*/*')

# df_green = spark.read.parquet('../code/data/pq/green/*/*')

df_yellow = spark.read.parquet(input_yellow)

df_green = spark.read.parquet(input_green)


df_yellow = df_yellow \
                .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_green = df_green \
                .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


common_columns = []

for col in df_green.columns:
    if col in set(df_yellow.columns):
        common_columns.append(col)
    else:
        pass


df_yellow_sel = df_yellow \
                    .select(common_columns) \
                    .withColumn('service_type', F.lit('yellow'))  #F.lit it used to fill the value as literally 

df_green_sel = df_green \
                    .select(common_columns) \
                    .withColumn('service_type', F.lit('green')) 



df_trips_data = df_green_sel.unionAll(df_yellow_sel)



df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# df_result.coalesce(1).write.parquet('../code/data/report/revenue/', mode='overwrite')

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()



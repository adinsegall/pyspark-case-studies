import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType, FloatType, IntegerType


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("nyc-taxi-analysis")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Path for NYC Yellow Taxi dataset
    yellow_taxi_path = f"s3a://{s3_data_repository_bucket}/data/nyc_taxi/yellow_tripdata/2023/"
    # Path for Taxi Zone Lookup file
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/data/nyc_taxi/taxi_zone_lookup.csv"


    try:
        # Load Taxi Zone Lookup data into a DataFrame
        taxi_zone_df = spark.read.csv(taxi_zone_lookup_path, header=True, inferSchema=True)
        print(f"Loaded Taxi Zone Lookup DataFrame with {taxi_zone_df.count()} rows.")
        taxi_zone_df.show(5)

        # Load all monthly NYC Yellow Taxi data into a DataFrame
        yellow_taxi_df = spark.read.csv(yellow_taxi_path, header=True, inferSchema=True)
        total_entries = yellow_taxi_df.count()
        print(f"Total number of entries in the NYC Yellow Taxi DataFrame: {total_entries}")
        yellow_taxi_df.show(5)
    
        # Join yellow_taxi_df with taxi_zone_df on PULocationID
        joined_df = (
            yellow_taxi_df
            .join(taxi_zone_df, yellow_taxi_df["PULocationID"] == taxi_zone_df["LocationID"], "inner")
            .withColumnRenamed("Borough", "Pickup_Borough")
            .withColumnRenamed("Zone", "Pickup_Zone")
            .withColumnRenamed("service_zone", "Pickup_service_zone")
            .drop("LocationID", "PULocationID")
        )

        print("Joined DataFrame after PULocationID join:")
        joined_df.show(5)

        # Perform a second join on DOLocationID using an inner join
        final_df = (
            joined_df
            .join(taxi_zone_df.withColumnRenamed("Borough", "Dropoff_Borough")
                               .withColumnRenamed("Zone", "Dropoff_Zone")
                               .withColumnRenamed("service_zone", "Dropoff_service_zone"),
                  joined_df["DOLocationID"] == taxi_zone_df["LocationID"], "inner")
            .drop("LocationID", "DOLocationID")
        )

        # Add "route" and "Month" columns to the final DataFrame (Q4)
        final_df = (
            final_df
            .withColumn("route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough")))
            .withColumn("Month", month(to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")))
        )

        # Cast monetary columns to DecimalType
        money_type = DecimalType(10, 2)

        final_df = final_df.withColumn("tip_amount", col("tip_amount").cast(money_type)) \
                           .withColumn("total_amount", col("total_amount").cast(money_type)) \
                           .withColumn("fare_amount", col("fare_amount").cast(money_type)) \
                           .withColumn("extra", col("extra").cast(money_type)) \
                           .withColumn("mta_tax", col("mta_tax").cast(money_type)) \
                           .withColumn("tolls_amount", col("tolls_amount").cast(money_type)) \
                           .withColumn("congestion_surcharge", col("congestion_surcharge").cast(money_type))

        print(f"ROUTE AND MONTH THING {final_df.count()} rows.")
        final_df.show(10)


        # Ensure passenger_count is numeric and handle nulls
        final_df = (
            final_df
            .withColumn("passenger_count", col("passenger_count").cast("float"))
            .fillna({"passenger_count": 0})
        )

        
        # Show 10 rows with 'route' and 'Month' fields (with timestamps, no truncation)
        print("Final DataFrame with 'route' and 'Month' columns:")
        final_df.select("tpep_pickup_datetime", "route", "Month").show(10, truncate=False)
                

        #Print schema for  the final dataframe (Q3 - p4)
        print("Schema of the Final DataFrame:")
        final_df.printSchema()
        
        #group by month and route, aggregate tip amount and passenger count (Q5)
        aggregated_df = (
            final_df
            .groupBy("Month", "route")
            .agg(
                sum("tip_amount").alias("total_tip_amount"),
                sum("passenger_count").alias("total_passenger_count")
            )
            .withColumn(
                "average_tip_per_passenger",
                when(col("total_passenger_count") > 0, col("total_tip_amount") / col("total_passenger_count"))
                .otherwise(lit(0))
            )
        )


        #cast avg_tip_per_passenger to decimaltype
        aggregated_df = aggregated_df.withColumn(
            "average_tip_per_passenger", col("average_tip_per_passenger").cast(DecimalType(10,2))
        )
        
        # print results for q5
        print("Aggregated DataFrame with total tip amount, total passenger count, and average tip per passenger:")
        aggregated_df.select(
            "Month",
            "route",
            "total_tip_amount",
            "total_passenger_count",
            "average_tip_per_passenger"
        ).show(10, truncate=False)


        # Filter entries where average_tip_per_passenger is 0 (Q6)
        zero_tip_df = aggregated_df.filter(col("average_tip_per_passenger") == 0)

        # Print results for Q6
        print("Entries where average_tip_per_passenger is 0:")
        zero_tip_df.select(
            "Month",
            "route",
            "total_tip_amount",
            "total_passenger_count",
            "average_tip_per_passenger"
        ).show(10, truncate=False)



        # Filter and process the data
        filtered_df = (
            yellow_taxi_df
            .filter((col("fare_amount") > 50) & (col("trip_distance") < 1))
            .withColumn("date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
            .filter((col("date") >= lit("2023-02-01")) & (col("date") <= lit("2023-02-07")))
            .groupBy("date")
            .agg(count("*").alias("trip_count"))
            .orderBy("date")
        )

        # Show the results
        print("Trips with fare_amount > $50 and trip_distance < 1 mile from 2023-02-01 to 2023-02-07:")
        filtered_df.show(truncate=False)


        # Q7: Top 10 routes by average_tip_per_passenger
        top_10_routes_df = (
            aggregated_df
            .orderBy(col("average_tip_per_passenger").desc())  # Sort descending by average_tip_per_passenger
            .limit(10)  # Limit to top 10 rows
        )

        # Print results for Q7
        print("Top 10 routes by average_tip_per_passenger:")
        top_10_routes_df.select(
            "Month",
            "route",
            "total_tip_amount",
            "total_passenger_count",
            "average_tip_per_passenger"
        ).show(10, truncate=False)



    except Exception as e:
        print(f"Error accessing bucket: {e}")




    spark.stop()
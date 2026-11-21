import os
import sys
import string
import math
import socket
from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, \
    year, countDistinct, expr, round, unix_timestamp, udf
from pyspark.sql.types import FloatType, IntegerType, DoubleType

print(f"Python executable being used: {sys.executable}")
print(f"Python paths: {sys.path}")

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("ethereum-analysis") \
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    #paths for datasets:
    blocks_path = f"s3a://{s3_data_repository_bucket}/data/ethereum/blocks.csv"
    transactions_path = f"s3a://{s3_data_repository_bucket}/data/ethereum/transactions.csv"

    try:
        print("Python version:", sys.version)


        #Q1 - Load blocks.csv and transactions.csv
        
        # load blocks.csv into a DataFrame
        blocks_df = spark.read.csv(blocks_path, header=True, inferSchema=True)
        print("Schema of blocks.csv:")
        blocks_df.printSchema()

        # load transactions.csv into a DataFrame
        transactions_df = spark.read.csv(transactions_path, header=True, inferSchema=True)
        print("Schema of transactions.csv:")
        transactions_df.printSchema()

        #Q2 - Evaluate the top 10 miners by the total size of blocks they have mined in the blocks.csv file

        top_miners_df = (
            blocks_df.groupBy("miner")
            .agg(sum("size").alias("total_size"))
            .orderBy(col("total_size").desc())
            .limit(10)
        )
    
        #print results
        print("Top 10 miners by total size of blocks mined:")
        top_miners_df.show(truncate=False)
        
    
        #Q3 - convert the UNIX timestamp to the "yyyy-MM-dd" format, excluding the specific time of day (hh-mm-ss). Create a new field to store the converted date in the "yyyy-MM-dd" format. For example, convert '1441625245' to '2015-09-07'. Select the “timestamp” and “formatted_data” fields and show 10 sample records with the “timestamp” and “formatted_data” fields.

        #Convert UNIX timestamp to "yyyy-MM-dd" format
        blocks_with_date_df = blocks_df.withColumn(
            "formatted_date",
            to_date(from_unixtime(col("timestamp")))
        )

        # Select timestamp and formatted_date and display 10 rows
        print("Converted timestamp to formatted_date:")
        blocks_with_date_df.select("timestamp", "formatted_date").show(10, truncate=False)


        #Q4 - Use inner join to merge the two files by joining the “block_hash” field from “transactions.csv” with the “hash” field from “blocks.csv”
        
        #alias columns to avoid ambiguity
        transactions_df = transactions_df.alias("transactions")
        blocks_df = blocks_df.alias("blocks")

        
        merged_df = transactions_df.join(
            blocks_df, 
            transactions_df["block_hash"] == blocks_df["hash"], 
            "inner"
        )

        # Count the number of lines in the merged dataset
        merged_count = merged_df.count()

        # Print the number of lines in the merged dataset
        print(f"The number of lines in the merged dataset is: {merged_count}")

        # Q5 - Extract entries for September 2015
        september_data = (
            merged_df.withColumn("formatted_date", to_date(from_unixtime(col("blocks.timestamp"))))
            .filter((col("formatted_date") >= "2015-09-01") & (col("formatted_date") <= "2015-09-30"))
            .groupBy("formatted_date")
            .agg(
                countDistinct("blocks.hash").alias("block_count"),  #count distinct blocks
                countDistinct("transactions.from_address").alias("unique_senders_count_number")  #count unique senders
            )
            .orderBy("formatted_date")
        )

        # Show results for September 2015
        print("September 2015 block and unique senders data:")
        september_data.show(truncate=False)

        # Collect the data to plot histograms
        september_data_local = september_data.toPandas()

        # Save CSV to plot block_count and unique_senders_count_number histogram
        september_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3a://{s3_bucket}/september_2015_data")
        print(f"September 2015 data saved to: s3a://{s3_bucket}/september_2015_data/")



        # Q6 - Extract October 2015 entries and calculate transaction fees
        from pyspark.sql.types import DecimalType
        
        october_fees = (
            merged_df.withColumn("formatted_date", to_date(from_unixtime(col("blocks.timestamp"))))
            .filter((col("formatted_date") >= "2015-10-01") & (col("formatted_date") <= "2015-10-31"))
            .filter(col("transactions.transaction_index") == 0)
            .withColumn("gas_decimal", col("transactions.gas").cast(DecimalType(38, 0)))
            .withColumn("gas_price_decimal", col("transactions.gas_price").cast(DecimalType(38, 0)))
            .withColumn("transaction_fee", col("gas_decimal") * col("gas_price_decimal"))
            .groupBy("formatted_date")
            .agg(sum("transaction_fee").alias("total_transaction_fee"))
            .orderBy("formatted_date")
        )
        
        # Show results for October 2015
        print("October 2015 transaction fees (transaction_index = 0):")
        october_fees.show(31, truncate=False)  # Show all October days
        
        # Save October data to CSV
        october_fees.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3a://{s3_bucket}/october_2015_fees")
        print(f"October 2015 fees data saved to: s3a://{s3_bucket}/october_2015_fees/")

    except Exception as e:
        print(f"Error during timestamp conversion: {e}")

    # now = datetime.now()
    # date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    # rideshare_tripZone_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/merged_data_" + date_time + ".csv", header=True)
    # rideshare_tripZone_df.write.csv("s3a://" + s3_bucket + "/processed_data_" + date_time, header=True)

    spark.stop()
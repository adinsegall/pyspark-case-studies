import sys, string
import os
import socket
import time
import operator
import boto3
import json
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, Row
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Global batch counter for tracking
batch_counter = {"q4": 0, "q5": 0, "q6": 0}

def process_batch_q4(df, epoch_id):
    # Custom batch processor for Q4 to track batch numbers
    global batch_counter
    print(f"\n{'='*45}")
    print(f"Batch: {epoch_id}")
    print(f"{'='*45}")
    batch_counter["q4"] = epoch_id

def process_batch_q5(df, epoch_id):
    # Custom batch processor for Q5 to track batch numbers
    global batch_counter
    print(f"\n{'='*45}")
    print(f"Batch: {epoch_id}")
    print(f"{'='*45}")
    batch_counter["q5"] = epoch_id

def process_batch_q6(df, epoch_id):
    # Custom batch processor for Q6 to track batch numbers
    global batch_counter
    print(f"\n{'='*45}")
    print(f"Batch: {epoch_id}")
    print(f"{'='*45}")
    batch_counter["q6"] = epoch_id

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession\
        .builder\
        .appName("Task4_NASA_Logs_Streaming_Robust")\
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # S3 Configuration
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    # Configure Hadoop for S3 access
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Streaming configuration
    streaming_host = "stream-emulator.data-science-tools.svc.cluster.local"
    streaming_port = 5551
    
    
    
    
    # Questions 1, 2 & 3
    print("\nQuestion 1 & 2: Loading NASA logs and defining watermark...")
    print(f"Connecting to: {streaming_host}:{streaming_port}")
    
    # Create stream with timestamp
    logsDF = spark.readStream \
        .format("socket") \
        .option("host", streaming_host) \
        .option("port", streaming_port) \
        .option('includeTimestamp', 'true') \
        .load()
    
    # Question 2: Add watermark
    logsDF = logsDF.withWatermark("timestamp", "3 seconds")
    print("Watermark defined: 3 seconds delay")
    print("Purpose: Watermark handles late-arriving data by specifying a time threshold,")
    print("after which delayed data is considered too late to be processed and is discarded.")
    
    # Parse the data
    logsDF = logsDF.select(explode(split(logsDF.value, "\n")).alias("logs"), logsDF.timestamp)
    
    parsedLogsDF = logsDF.withColumn('row_index', split(logsDF['logs'], ',').getItem(0)) \
        .withColumn('hostname', split(logsDF['logs'], ',').getItem(1)) \
        .withColumn('log_timestamp', split(logsDF['logs'], ',').getItem(2)) \
        .withColumn('method', split(logsDF['logs'], ',').getItem(3)) \
        .withColumn('url', split(logsDF['logs'], ',').getItem(4)) \
        .withColumn('response', split(logsDF['logs'], ',').getItem(5)) \
        .withColumn('bytes', split(logsDF['logs'], ',').getItem(6)) \
        .drop('logs')
    
    print("\nQuestion 1 & 3: Displaying dataset in append mode...")
    
    # Start query for Q1 & Q3
    query1_3 = parsedLogsDF.writeStream \
        .outputMode("append") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Wait for batch 20
    time.sleep(210)
    
    query1_3.stop()
    
    # Sleep before next question
    time.sleep(10)







    
    # Question 4
    print("Question 4 - GIF Count")
    
    # Create fresh stream
    logsDF_q4 = spark.readStream \
        .format("socket") \
        .option("host", streaming_host) \
        .option("port", streaming_port) \
        .option('includeTimestamp', 'true') \
        .load()
    
    logsDF_q4 = logsDF_q4.withWatermark("timestamp", "3 seconds")
    logsDF_q4 = logsDF_q4.select(explode(split(logsDF_q4.value, "\n")).alias("logs"), logsDF_q4.timestamp)
    
    parsedDF_q4 = logsDF_q4.withColumn('url', split(logsDF_q4['logs'], ',').getItem(4))
    
    # Count GIFs with proper windowing
    gifCountDF = parsedDF_q4 \
        .filter(col("url").contains("gif")) \
        .groupBy(window(col("timestamp"), "60 seconds", "30 seconds")) \
        .agg(count("*").alias("gif_count"))

    
    # Use foreachBatch to track batch numbers
    def write_q4_batch(df, epoch_id):
        process_batch_q4(df, epoch_id)
        df.show(100, truncate=False)
    
    query4 = gifCountDF.writeStream \
        .outputMode("update") \
        .foreachBatch(write_q4_batch) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Wait for batch 3
    print("\nWaiting for batch 3...")
    while batch_counter["q4"] < 3:
        time.sleep(5)
    
    # Extra time to ensure batch 3 completes
    time.sleep(5)
    
    query4.stop()
    
    # Pause
    print("\nPausing 10 seconds before next part...")
    time.sleep(10)
    
    




    
    
    # Question 5
    print("Question 5 - Bytes Aggregation")
    
    # Create fresh stream
    logsDF_q5 = spark.readStream \
        .format("socket") \
        .option("host", streaming_host) \
        .option("port", streaming_port) \
        .option('includeTimestamp', 'true') \
        .load()
    
    logsDF_q5 = logsDF_q5.withWatermark("timestamp", "3 seconds")
    logsDF_q5 = logsDF_q5.select(explode(split(logsDF_q5.value, "\n")).alias("logs"), logsDF_q5.timestamp)
    
    parsedDF_q5 = logsDF_q5 \
        .withColumn('hostname', split(logsDF_q5['logs'], ',').getItem(1)) \
        .withColumn('bytes', split(logsDF_q5['logs'], ',').getItem(6))
    
    # Convert bytes to integer
    parsedDF_q5 = parsedDF_q5.withColumn("bytes", 
        when(col("bytes").isNotNull() & (col("bytes") != "") & (col("bytes") != "bytes"), 
             col("bytes").cast("integer"))
        .otherwise(0))
    
    # Aggregate with proper windowing
    bytesAggDF = parsedDF_q5 \
        .filter(col("bytes") > 0) \
        .groupBy(
            window(col("timestamp"), "50 seconds", "30 seconds"),
            col("hostname")
        ) \
        .agg(sum("bytes").alias("total_bytes")) \
        .orderBy(col("total_bytes").desc())
    
    # Use foreachBatch to track batch numbers
    def write_q5_batch(df, epoch_id):
        process_batch_q5(df, epoch_id)
        df.show(100, truncate=False)
    
    query5 = bytesAggDF.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_q5_batch) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Wait for batch 4
    print("\nWaiting for batch 4...")
    while batch_counter["q5"] < 4:
        time.sleep(5)
    
    # Extra time to ensure batch 4 completes
    time.sleep(5)
    
    query5.stop()
    
    # Pause
    print("\nPausing 10 seconds before next part...")
    time.sleep(10)
    
    
    
    
    # Question 6
    print("Question 6 - Successful GET Requests")
    
    # Create fresh stream
    logsDF_q6 = spark.readStream \
        .format("socket") \
        .option("host", streaming_host) \
        .option("port", streaming_port) \
        .option('includeTimestamp', 'true') \
        .load()
    
    logsDF_q6 = logsDF_q6.withWatermark("timestamp", "3 seconds")
    logsDF_q6 = logsDF_q6.select(explode(split(logsDF_q6.value, "\n")).alias("logs"), logsDF_q6.timestamp)
    
    parsedDF_q6 = logsDF_q6 \
        .withColumn('hostname', split(logsDF_q6['logs'], ',').getItem(1)) \
        .withColumn('method', split(logsDF_q6['logs'], ',').getItem(3)) \
        .withColumn('response', split(logsDF_q6['logs'], ',').getItem(5))
    
    # Filter and count
    successfulGetsDF = parsedDF_q6 \
        .filter((col("method") == "GET") & (col("response") == "200")) \
        .groupBy("hostname") \
        .agg(count("*").alias("Correct_count"))
    
    # Use foreachBatch to track batch numbers
    def write_q6_batch(df, epoch_id):
        process_batch_q6(df, epoch_id)
        df.show(100, truncate=False)
    
    query6 = successfulGetsDF.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_q6_batch) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Wait for batch 6
    print("\nWaiting for batch 6...")
    while batch_counter["q6"] < 6:
        time.sleep(5)
    
    # Extra time to ensure batch 6 completes
    time.sleep(5)
    
    query6.stop()
    
    # Stop Spark session
    spark.stop()
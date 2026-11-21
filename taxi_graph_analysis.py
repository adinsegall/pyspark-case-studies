import sys, string
import os
import socket
import time
import operator
import boto3
import json
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from graphframes import *

if __name__ == "__main__":
    # Initialize Spark Session with GraphFrames package
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")\
        .appName("Task3_NYC_Green_Taxi_GraphFrames")\
        .getOrCreate()
    
    # Set log level to reduce noise in output
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
    
    # Dataset paths
    green_taxi_path = f"s3a://{s3_data_repository_bucket}/data/nyc_taxi/green_tripdata/2023/"
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/data/nyc_taxi/taxi_zone_lookup.csv"
    
    print("=" * 50)
    print("Task 3: NYC Green Taxi GraphFrames Analysis")
    print("=" * 50)



    
    # Question 1: Load all monthly CSV files

    now = datetime.now()
    print("Current datetime:", now)
    print("\nQuestion 1: Loading all monthly NYC Green Taxi CSV files...")
    print("-" * 40)
    
    # Define schema for Green Taxi data
    green_taxi_schema = StructType([
        StructField("lpep_pickup_datetime", StringType(), True),
        StructField("lpep_dropoff_datetime", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("ehail_fee", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("taxi_type", StringType(), True)
    ])
    
    # Load all monthly CSV files
    green_taxi_df = spark.read \
        .option("header", "true") \
        .schema(green_taxi_schema) \
        .csv(green_taxi_path + "*.csv")
    
    # Count total entries
    total_entries = green_taxi_df.count()
    print(f"Total number of entries in NYC Green Taxi dataset: {total_entries:,}")
    now = datetime.now()
    print("Current datetime:", now)
    
    # Cache the dataframe for better performance
    green_taxi_df.cache()





    
    # Question 2 (5 points): Define vertices and edges DataFrames

    print("\nQuestion 2: Creating vertices and edges DataFrames...")
    print("-" * 40)
    
    # Load taxi zone lookup data
    taxi_zone_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(taxi_zone_lookup_path)
    
    # Create vertices DataFrame from taxi_zone_lookup
    vertices = taxi_zone_df.select(
        col("LocationID").alias("id"),
        col("Borough"),
        col("Zone"),
        col("service_zone")
    )
    
    print("Vertices DataFrame (5 samples):")
    vertices.show(5, truncate=False)
    
    # Create edges DataFrame from taxi trip data
    # Filter out null values and create edges
    edges = green_taxi_df \
        .filter(col("PULocationID").isNotNull() & col("DOLocationID").isNotNull()) \
        .select(
            col("PULocationID").alias("src"),
            col("DOLocationID").alias("dst")
        ) \
        .distinct()  # Get unique edges
    
    print("\nEdges DataFrame (5 samples):")
    edges.show(5, truncate=False)
    
    print(f"\nTotal vertices: {vertices.count()}")
    print(f"Total unique edges: {edges.count()}")
    now = datetime.now()
    print("Current datetime:", now)




    
    # Question 3 (4 points): Create graph and show samples
    
    print("\nQuestion 3: Creating graph and displaying samples...")
    print("-" * 40)
    
    # Create GraphFrame
    graph = GraphFrame(vertices, edges)
    
    # Display 10 samples with src, edge, and dst columns
    # Join edges with vertices to show the complete graph structure
    graph_samples = graph.edges \
        .join(graph.vertices, graph.edges.src == graph.vertices.id, "left") \
        .select(
            graph.edges.src,
            concat_ws("->", graph.edges.src, graph.edges.dst).alias("edge"),
            graph.edges.dst
        )
    
    print("Graph DataFrame samples (10 rows):")
    graph_samples.show(10, truncate=False)
    now = datetime.now()
    print("Current datetime:", now)







    
    # Question 4 (5 points): Count connected vertices with same Borough and service_zone

    print("\nQuestion 4: Finding connected vertices with same Borough and service_zone...")
    print("-" * 40)
    
    # Join edges with vertices for both source and destination
    connected_same_borough_zone = graph.edges \
        .join(vertices.alias("v1"), col("src") == col("v1.id")) \
        .join(vertices.alias("v2"), col("dst") == col("v2.id")) \
        .filter((col("v1.Borough") == col("v2.Borough")) & 
                (col("v1.service_zone") == col("v2.service_zone"))) \
        .select(
            col("src").alias("id"),
            col("dst").alias("id_dst"),
            col("v1.Borough").alias("Borough"),
            col("v1.service_zone").alias("service_zone")
        )
    
    # Count total connected vertices meeting the condition
    total_connected = connected_same_borough_zone.count()
    print(f"Total number of connected vertices with same Borough and service_zone: {total_connected}")
    
    print("\n10 samples of connected vertices:")
    connected_same_borough_zone.show(10, truncate=False)
    now = datetime.now()
    print("Current datetime:", now)
    
    # Save results for Question 4
    q4_output_path = f"s3a://{s3_bucket}/task3_q4_connected_vertices"
    connected_same_borough_zone.coalesce(1).write.mode("overwrite").option("header", "true").csv(q4_output_path)
    




    
    # Question 5 (4 points): Find shortest paths to vertex 1

    print("\nQuestion 5: Finding shortest paths from all vertices to vertex 1...")
    print("-" * 40)
    
    # Use shortest paths algorithm to find paths to LocationID = 1
    shortest_paths = graph.shortestPaths(landmarks=[1])
    
    # Process results to show id and shortest distance
    # shortestPaths returns a DataFrame, not a GraphFrame
    shortest_paths_result = shortest_paths \
        .filter(col("id") != 1) \
        .select(
            col("id").alias("id_to_1"),
            col("distances")[1].alias("shortest_distance")
        ) \
        .filter(col("shortest_distance").isNotNull())
    
    print("Shortest paths to vertex 1 (10 samples):")
    shortest_paths_result.orderBy("shortest_distance").show(10, truncate=False)
    now = datetime.now()
    print("Current datetime:", now)
    # Save results for Question 5
    q5_output_path = f"s3a://{s3_bucket}/task3_q5_shortest_paths"
    shortest_paths_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(q5_output_path)





    

    # Question 6 (5 points): PageRank algorithm
    print("\nQuestion 6: Performing PageRank on the graph...")
    print("-" * 40)
    
    # Run PageRank algorithm with specified parameters
    pagerank_results = graph.pageRank(resetProbability=0.17, tol=0.01)
    
    # Get top 5 vertices by PageRank value
    # pageRank returns a GraphFrame, so we need to access vertices
    top_pagerank = pagerank_results.vertices \
        .select("id", "pagerank") \
        .orderBy(col("pagerank").desc()) \
        .limit(5)
    
    print("Top 5 vertices by PageRank:")
    top_pagerank.show(truncate=False)
    
    # Save results for Question 6
    q6_output_path = f"s3a://{s3_bucket}/task3_q6_pagerank"
    pagerank_results.vertices.select("id", "pagerank").orderBy(col("pagerank").desc()).coalesce(1).write.mode("overwrite").option("header", "true").csv(q6_output_path)
    
    
    print("Current datetime:", now)
    
    # Stop Spark session
    spark.stop()
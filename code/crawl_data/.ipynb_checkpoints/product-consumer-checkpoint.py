import json
import re
from tqdm import tqdm
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import argparse

from json import dumps
from kafka import KafkaConsumer
from json import loads
from pyspark.sql import SparkSession
import os

# define environment kafka streaming
kafka = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"
spark = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0"

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages {},{} pyspark-shell".format(kafka, spark))

parser = argparse.ArgumentParser()
parser.add_argument('--topic', type=str, default='Product')

args = parser.parse_args()



if __name__ == '__main__':

    # init spark session
    spark = SparkSession.\
        builder.\
        appName("Crawl-Product").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
    

    KAFKA_BROKER='kafka-1:9092'
    
    KAFKA_TOPIC= args.topic
    kafkaMessages = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_BROKER) \
      .option("subscribe", KAFKA_TOPIC) \
      .option("startingOffsets", "earliest") \
      .load()
    
    message = kafkaMessages.selectExpr("CAST(value AS STRING)")

    fileStream = message.writeStream \
                      .trigger(processingTime='300 seconds')\
                      .queryName("Persist the processed data") \
                      .outputMode("append") \
                      .format("parquet") \
                      .option("path", f"hdfs://namenode:9000/tiki/{KAFKA_TOPIC}") \
                      .option("checkpointLocation", f"hdfs://namenode:9000/tiki/checkpoints_{KAFKA_TOPIC}") \
                      .start() \
                      .awaitTermination()

                
                
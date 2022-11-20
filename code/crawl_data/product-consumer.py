import json
import re
from tqdm import tqdm
import pandas as pd
import time
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
        appName(f"Crawl-{args.topic}").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
    

    KAFKA_BROKER_1='kafka-1:9092'
    KAFKA_BROKER_2='kafka-2:9092'
    KAFKA_BROKER_3='kafka-3:9092'
    
    KAFKA_TOPIC= args.topic
    kafkaMessages = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", f"{KAFKA_BROKER_1},{KAFKA_BROKER_2},{KAFKA_BROKER_3}") \
      .option("subscribe", KAFKA_TOPIC) \
      .option("startingOffsets", "earliest") \
      .load()
    
    message = kafkaMessages.selectExpr("CAST(value AS STRING)")

    fileStream = message.writeStream \
                      .trigger(processingTime='30 seconds')\
                      .queryName("Persist the processed data") \
                      .outputMode("append") \
                      .format("parquet") \
                      .option("path", f"hdfs://namenode:9000/tiki/{KAFKA_TOPIC}") \
                      .option("checkpointLocation", f"hdfs://namenode:9000/tiki/checkpoints_{KAFKA_TOPIC}") \
                      .start()
    while True:
        time.sleep(5)
        if fileStream.lastProgress != None:
            if fileStream.lastProgress['numInputRows'] == 0:
                print('No data inseart in Kafka -> Stop spark...')
                fileStream.stop()
                spark.stop()
                break
            else:
                print('Last procesing item:', fileStream.lastProgress['numInputRows'])
                time.sleep(10)
                
                
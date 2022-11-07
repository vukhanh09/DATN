from tiki_crawler import Tiki
import time
import requests
import json
import re
from tqdm import tqdm
import pandas as pd
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
import argparse


from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType
from kafka import KafkaProducer
from json import dumps



parser = argparse.ArgumentParser()
parser.add_argument('--topic', type=str, default='ShopInfo')
args = parser.parse_args()



if __name__ == '__main__':
    # init tiki crawler
    tiki = Tiki()
    
    # init kafka producer
    producer = KafkaProducer(bootstrap_servers=['kafka-1:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    try:
        # init Spark Session
        spark = SparkSession.\
            builder.\
            appName("get-product").\
            master("spark://spark-master:7077").\
            config("spark.executor.memory", "1024m").\
            getOrCreate()
    
    # read product from HDFS
        data = spark.read.parquet('hdfs://namenode:9000/tiki/Product')
        
        # defined schema for product
        
        
        schema = StructType([ 
            # get only current_seller info
            StructField("current_seller",MapType(StringType(),StringType()),True),
        ])
        
        df = data.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")
        df.createOrReplaceTempView('Product')
        
        shop_id = spark.sql("""select distinct current_seller.id from Product""").collect()
        
        #convert row rdd to list python
        shop_ids = [int(item.id) for item in shop_id]
        
        spark.stop()
        for id in shop_ids:
            shop_url = tiki.shop_api.format(id)
            try:
                shop_info = tiki.get_shop_information(shop_url)
                producer.send(args.topic, value=shop_info)
            except Exception as e:
                print(e)
                continue
        
    except Exception as e:
        print(e)

                
                
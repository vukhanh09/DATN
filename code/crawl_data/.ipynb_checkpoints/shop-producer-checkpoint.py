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


from pyspark.sql.functions import col,from_json,explode
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType
from kafka import KafkaProducer
from json import dumps



parser = argparse.ArgumentParser()
parser.add_argument('--topic', type=str, default='ShopInfo')
args = parser.parse_args()

def extrct_other_seller(arr,current_id):
    list_id = []
    list_id.append(current_id)
    for item in arr:
        list_id.append(item['id'])
    return list_id


if __name__ == '__main__':
    # init tiki crawler
    tiki = Tiki()
    
    # init kafka producer
    producer = KafkaProducer(bootstrap_servers=['kafka-1:9092','kafka-2:9092','kafka-3:9092'],
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
            StructField("other_sellers",ArrayType(MapType(StringType(),StringType())),True),

        ])
        
        df = data.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")
        df.createOrReplaceTempView('Product')
        
        spark.udf.register('extrct_other_seller',extrct_other_seller,ArrayType(StringType()))
        
        shop_id = spark.sql("""select distinct explode(extrct_other_seller(other_sellers,current_seller.id)) id from Product""").collect()
        
        #convert row rdd to list python
        shop_ids = [int(item.id) for item in shop_id if item.id != None]
        
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

                
                
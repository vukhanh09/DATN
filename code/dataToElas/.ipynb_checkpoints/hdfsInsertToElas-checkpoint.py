from json import loads
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import col,from_json,udf,split,explode
from pyspark.ml.feature import NGram
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType

import os 
es = "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1"
http_scala='org.apache.httpcomponents:httpclient:4.5.13'
common_client='commons-httpclient:commons-httpclient:3.1'

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages {},{},{} pyspark-shell".format(es, http_scala,common_client))



spark = SparkSession.\
        builder.\
        appName("hdfsToElasticsearch").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()


list_item = ['Product_Shop','metaData','comment_1gram','comment_2gram']



hdfs_url = 'hdfs://namenode:9000/analysis/{}'

for item in list_item:
    print(f'Start reading {item}...')
    data_path = hdfs_url.format(item)
    df_data = spark.read.parquet(data_path)
    
    df_data.write.format(
            "org.elasticsearch.spark.sql"
        ).option(
            "es.resource", '%s/%s' % (item.lower(), 'data')
        ).option(
            "es.nodes", 'es01'
        ).option(
            "es.port", 9200
        ).save()

    print(f'Inserted {item} data into Elasticsearch...')

spark.stop()
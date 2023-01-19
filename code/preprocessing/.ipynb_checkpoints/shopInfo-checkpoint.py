from json import loads
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import col,from_json,udf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType,BooleanType

spark = SparkSession.\
        builder.\
        appName("process-data").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()


data = spark.read.parquet('hdfs://namenode:9000/tiki/ShopInfo')

schema = StructType([ 
    StructField("id",IntegerType(),True), 
    StructField("store_id",IntegerType(),True), 
    StructField("name",StringType(),True), 
    StructField("icon",StringType(),True),
    StructField("url",StringType(),True), 
    StructField("is_official",BooleanType(),True),
    StructField("store_level",StringType(),True),
    StructField('is_followed', BooleanType(),True),
    StructField('avg_rating_point', FloatType(),True),
    StructField('review_count', IntegerType(),True),
    StructField("total_follower",IntegerType(),True), 
    StructField("days_since_joined",IntegerType(),True), 
])

df = data.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

df.createOrReplaceTempView('shopinfo')
df.write.mode('overwrite').parquet('hdfs://namenode:9000/TikiCleaned/ShopInfo')
spark.stop()
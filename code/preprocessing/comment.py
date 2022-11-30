from json import loads
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import col,from_json,udf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType

spark = SparkSession.\
        builder.\
        appName("process-data").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()

data = spark.read.parquet('hdfs://namenode:9000/tiki/Comment')

schema = StructType([ 
    StructField("id",IntegerType(),True), 
    StructField("content",StringType(),True), 
    StructField("thank_count",IntegerType(),True), 
    StructField("customer_id",IntegerType(),True),
    StructField("rating",IntegerType(),True), 
    StructField("created_by",MapType(StringType(),StringType()),True),
    StructField("spid",IntegerType(),True),
    StructField("seller",MapType(StringType(),StringType()),True),
    StructField("timeline",MapType(StringType(),StringType()),True),
    StructField('product_id', IntegerType(),True),
])

df = data.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

df.createOrReplaceTempView('Comment')

import re
# as per recommendation from @freylis, compile once only
CLEANR = re.compile('<.*?>') 
def cleanText(str_raw):
    # remove tags html
    str_raw = re.sub(CLEANR, ' ', str_raw)

    # remove special character
    str_raw = re.sub('\W+', ' ', str_raw)
    
    # remove number
    str_raw = re.sub("[0-9]+", "", str_raw)
    
    # remove space
    cleantext = re.sub(" +", " ", str_raw)
    return cleantext.lower()

spark.udf.register("cleanText", cleanText,StringType())

df_clean = spark.sql("""
    select id,content content,cleanText(content) clean_content,thank_count,customer_id,rating,spid,
    seller.id seller_id,product_id, created_by.full_name customer_full_name,created_by.purchased_at purchased_at,
    from_unixtime(CAST(created_by.purchased_at as BIGINT), 'yyyy-MM-dd') date_purchased_at,
    timeline.review_created_date review_created_date,timeline.delivery_date delivery_date,
    datediff(timeline.review_created_date,timeline.delivery_date) review_after_delivery,
    case
        when rating >= 4 then 'Positive'
        when rating = 3 then 'Neutral'
        when rating <3 then 'Negative'
    end as sentiment,
    case
        when rating >= 4 then 2
        when rating = 3 then 1
        when rating <3 then 0
    end as lable
    from Comment c
""")

df_clean.write.mode('append').parquet('hdfs://namenode:9000/TikiCleaned/Comment')
spark.stop()
from json import loads
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import col,from_json,udf,explode
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType
import re

spark = SparkSession.\
        builder.\
        appName("process-data").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()

data = spark.read.parquet('hdfs://namenode:9000/tiki/Product')

schema = StructType([ 
    StructField("id",IntegerType(),True), 
    StructField("master_id",IntegerType(),True), 
    StructField("sku",StringType(),True), 
    StructField("name",StringType(),True),
    StructField("short_description",StringType(),True), 
    StructField("price",IntegerType(),True),
    StructField("list_price",IntegerType(),True),
    StructField('original_price', IntegerType(),True),
    StructField('discount', IntegerType(),True),
    StructField('discount_rate', FloatType(),True),
    
    StructField("rating_average",FloatType(),True), 
    StructField("review_count",IntegerType(),True), 
    StructField("productset_group_name",StringType(),True), 
    StructField("all_time_quantity_sold",IntegerType(),True),
    
    StructField("description",StringType(),True), 
    StructField("current_seller",MapType(StringType(),StringType()),True),
    StructField("other_sellers",ArrayType(MapType(StringType(),StringType())),True),
    StructField("breadcrumbs",ArrayType(MapType(StringType(),StringType())),True),
    StructField("specifications",ArrayType(StructType(
                    [
                        StructField("name", StringType()),
                        StructField("attributes",ArrayType(MapType(StringType(),StringType())),True),
                    ]
                )),True),

    StructField('return_and_exchange_policy', StringType(),True)
])

df = data.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

df.createOrReplaceTempView('Product')
def parserAtt(specifications):
    result = ""
    try:
        for s in specifications:
            for a in s.attributes:
                result += a['value']
        result = cleanText(result)
    except:
        return ""
    return result

spark.udf.register('parserAtt',parserAtt,StringType())



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

product_clean = spark.sql("""
        select id,master_id,sku,price,list_price,original_price,discount,discount_rate,
        rating_average,review_count,productset_group_name,all_time_quantity_sold,
        name, short_description,
        cleanText(name) clean_name,cleanText(description) clean_description,parserAtt(specifications) clean_specifications,
        breadcrumbs[0].name category_name,breadcrumbs[0].category_id category_id,
        current_seller.id seller_id,current_seller.name seller_name,current_seller.store_id seller_store_id,
        cast(current_seller.product_id as int) spid
        from Product
""")

product_clean.write.partitionBy("category_id").mode('append').parquet('hdfs://namenode:9000/TikiCleaned/Product')
spark.stop()
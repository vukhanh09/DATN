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

df_product = spark.read.parquet('hdfs://namenode:9000/TikiCleaned/Product')
df_comment = spark.read.parquet('hdfs://namenode:9000/TikiCleaned/Comment')
df_shop = spark.read.parquet('hdfs://namenode:9000/TikiCleaned/ShopInfo')

df_product.createOrReplaceTempView("product")
df_comment.createOrReplaceTempView("comment")
df_shop.createOrReplaceTempView("shop")

print("comment count: {} \nproduct count: {} \nshop count: {}".format(df_comment.count(),df_product.count(),
                                       df_shop.count()))


df_product_shop = spark.sql("""
    select p.id p_id, p.master_id,p.price,p.list_price,p.original_price,p.discount,p.discount_rate,
    p.productset_group_name,p.all_time_quantity_sold,p.name p_name,p.short_description,p.clean_name p_clean_name,
    p.clean_description,p.clean_specifications,
    p.category_name,p.category_id,
    p.seller_id, s.store_id,s.name s_name,s.icon,s.url,s.is_official,s.store_level,s.is_followed,
    s.avg_rating_point,s.review_count, s.total_follower,s.days_since_joined
    from product p
    join shop s on p.seller_id = s.id
""")

df_product_shop.repartition(10).write.partitionBy("category_id").mode('overwrite').parquet('hdfs://namenode:9000/TikiCleaned/Product_Shop')

df_all = spark.sql("""
    select p.id p_id, p.master_id,p.price,p.list_price,p.original_price,p.discount,p.discount_rate,
    p.productset_group_name,p.all_time_quantity_sold,p.name p_name,p.short_description,p.clean_name p_clean_name,
    p.clean_description,p.clean_specifications,
    p.category_name,p.category_id,
    p.seller_id, s.store_id,s.name s_name,s.icon,s.url,s.is_official,s.store_level,s.is_followed,
    s.avg_rating_point,s.review_count, s.total_follower,s.days_since_joined, 
    c.id c_id,c.content, c.clean_content,c.thank_count,c.customer_id,c.rating,
    c.customer_full_name,c.purchased_at,c.date_purchased_at,c.review_created_date,
    c.delivery_date,c.review_after_delivery,c.sentiment,c.lable
    from product p
    join shop s on p.seller_id = s.id
    join comment c on c.product_id = p.id and p.seller_id = c.seller_id
""")

df_all.repartition(10).write.partitionBy("category_id").mode('overwrite').parquet('hdfs://namenode:9000/TikiCleaned/metaData')

spark.stop()
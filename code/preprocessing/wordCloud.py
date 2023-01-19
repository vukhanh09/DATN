from json import loads
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.functions import col,from_json,udf,split,explode
from pyspark.ml.feature import NGram
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,MapType,FloatType,ArrayType




spark = SparkSession.\
        builder.\
        appName("hdfsToElasticsearch").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()



df_comment = spark.read.parquet('hdfs://namenode:9000/TikiCleaned/Comment')
df_comment = df_comment.withColumn('comment_term',split(df_comment.clean_content, ' ', -1))


def getNGram(n):
    ngram = NGram(n=n)
    ngram.setInputCol("comment_term")
    ngram.setOutputCol("nGrams")
    df_nGram = ngram.transform(df_comment)
    result_nGram = df_nGram.withColumn('word',explode(df_nGram.nGrams))\
        .groupBy(['sentiment','word'])\
        .count()
    return result_nGram

for i in range(1,4):
    print('Split n gram:',i)
    result_nGram = getNGram(i)
    
    result_nGram.repartition(10).write.mode('overwrite').parquet(f'hdfs://namenode:9000/TikiCleaned/comment_{i}gram')               
spark.stop()

print(f'Inserted data into HDFS...')
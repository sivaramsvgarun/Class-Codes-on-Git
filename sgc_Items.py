import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark import *
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark_1 = glueContext.spark_session
spark = spark_1.builder.appName(
    'Read CSV File into DataFrame') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.caseSensitive", "True") \
    .getOrCreate()
path = "s3://sgc-149/rawdata/*"
df = spark.read.json(path)
df1=df.withColumn("as_date",from_unixtime(unix_timestamp(col("ITEM_DATE"),'dd-MMM-yy hh:mm:ss aa'),'yyyy-MM-dd HH:mm:ss aa'))
# df1.show()
def getInterval(time):
    start = int(time.split(":")[0])
    return str(start)+"-"+str(start+1)
getIntervalUdf = udf(getInterval,StringType())

df2=df1.withColumn("date",from_unixtime(unix_timestamp(col('as_date'),'yyyy-MM-dd HH:mm:ss aa'),'yyyy-MM-dd'))\
    .withColumn("time",from_unixtime(unix_timestamp(col('as_date'),'yyyy-MM-dd HH:mm:ss aa'),'HH:mm:ss aa'))\
    .withColumn("Interval",getIntervalUdf("time"))\
    .withColumn("account_id", col('ACCOUNT_ID').cast(IntegerType()))

item_df1 = df2.groupBy('date','account_id','Interval').agg(count('*').alias('msg_per_hr'))

df3 = item_df1.withColumn("year", year(col('date').cast("timestamp")))\
    .withColumn('month', month(col('date').cast('timestamp')))\
    .withColumn("day", dayofmonth(col('date').cast('timestamp')))\
    .withColumn("hour", split(col('Interval'),'-')[0].cast(IntegerType()))\
    .withColumn("hour_interval", col('Interval'))\
    .withColumn("dayname", date_format(df2.date, "EEEE"))\
    .withColumn("week",weekofyear(col('date')))

df4=df3.withColumn("daytime",when((df3.hour >6) & (df3.hour <= 12), 'Morning').when((df3.hour > 12) & (df3.hour <= 17), 'Afternoon').when((df3.hour > 17) & (df3.hour <= 20), 'Evening').otherwise('Night'))
output_path = "s3:/sgc-149/items/"
s3_bucket = "sgc-149"
database_name="sgc"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
df4.write.option("compression", "snappy").option("overwriteSchema", "true").option("path", f"""s3://{s3_bucket}/sgc/sgc_items""").mode("overwrite").format("parquet").saveAsTable(f"{database_name}.items")
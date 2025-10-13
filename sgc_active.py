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
database_name = "sgc"
table_name = "items"

# Read Athena table into a Glue DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name
)

# Convert to Spark DataFrame if needed
df = dynamic_frame.toDF()
# df.show()
day = df.groupBy('date', 'Interval', 'hour', 'account_id', 'msg_per_hr').count()
# day.show()

def daycalculation():
    day = df.groupBy('date', 'Interval', 'hour', 'account_id', 'msg_per_hr').count() \
        .withColumn('messages_per_day',
                    sum("msg_per_hr").over(Window.partitionBy("date", "account_id").orderBy('hour')).cast(
                        IntegerType())) \
        .withColumn("week", weekofyear(col('date'))) \
        .withColumn("dayname", date_format(df.date, "EEEE")) \
        .withColumn("hour_interval", col('Interval')) \
        .withColumn("daytime", when((df.hour >= 6) & (df.hour < 12), 'Morning').when((df.hour >= 12) & (df.hour <= 17),
                                                                                     'Afternoon').when(
        (df.hour > 17) & (df.hour <= 23), 'Evening').otherwise('Night'))
    return day

day = daycalculation()
# day.show()
def max_messages_per_day_calculation():
    df2_max = day.withColumn('max_hourly_messages_per_day',
                             max('msg_per_hr').over(Window.partitionBy("date", "account_id")).cast(IntegerType()))
    return df2_max

maxi = max_messages_per_day_calculation()
# maxi.show()
def week_cal():
    df3_week = maxi.withColumn('messages_per_week',
                               sum('messages_per_day').over(Window.partitionBy("week", "account_id")).cast(
                                   IntegerType()))
    return df3_week

week = week_cal()
# week.show()

def daytime_cal(week):
    df_daytime = week.withColumn('messages_per_daytime', sum('msg_per_hr').over(
        Window.partitionBy("date", "daytime", "account_id").orderBy("daytime")).cast(IntegerType()))
    return df_daytime

daytime = daytime_cal(week)
# daytime.show()
defaultcoefficients = '''{
  'bm_messages_per_hour_weight': 0.05,
  'bm_messages_per_daytime_weight': 0.05,
  'bm_messages_per_day_weight': 0.05,
  'bm_max_hourly_messages_per_day_weight': 0.05,
  'bm_messages_per_week_weight': 0.05,
  'dt_messages_per_specific_daytime_weight': {'Morning': 0.05, 'Afternoon':0.05,'Evening':0.05,'Night':0.05},
  'dn_messages_per_specific_day_weight': {'Monday':0.05,'Tuesday': 0.05, 'Wednesday':0.05,'Thursday':0.05,'Friday':0.05,'Saturday':0.05,'Sunday':0.05},
  'dn_max_hourly_messages_per_specific_day_weight': {'Monday':0.05,'Tuesday': 0.05, 'Wednesday':0.05,'Thursday':0.05,'Friday':0.05,'Saturday':0.05,'Sunday':0.05}}'''

def active_coefficients(daytime, defaultcoefficients):
    df_active = daytime.withColumn("year", year(col('date').cast("timestamp"))).withColumn('month', month(
        col('date').cast('timestamp'))).withColumn("day", dayofmonth(col('date').cast('timestamp'))) \
        .withColumnRenamed("msg_per_hr", "messages_per_hour") \
        .withColumn('anomaly_update_coefficients', lit(defaultcoefficients)) \
        .withColumn('default_update_coefficients', lit(defaultcoefficients))
    return df_active

df5 = active_coefficients(daytime, defaultcoefficients).coalesce(4)
output_path = "s3:/sgc-149/active/"
s3_bucket = "sgc-149"
database_name="sgc"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
df5.write.option("compression", "snappy").option("overwriteSchema", "true").option("path", f"""s3://{s3_bucket}/sgc/active""").mode("overwrite").format("parquet").saveAsTable(f"{database_name}.active")
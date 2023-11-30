# import requests
# import ssl
import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    ArrayType,
    LongType,
    DoubleType,
)

API_URL = "https://api.statworx.com/covid"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("Covid Application").getOrCreate()


# def api_response(url):
#     try:
#         response = requests.get(url)
#     except:
#         return response.json()


# response = api_response(API_URL)

# schema = StructType(
#     [
#         StructField("date", ArrayType(DateType(), True), True),
#         StructField("year_week", ArrayType(DateType(), True), True),
#         StructField("cases", ArrayType(IntegerType(), True), True),
#         StructField("deaths", ArrayType(IntegerType(), True), True),
#         StructField("country", StringType(), True),
#         StructField("code", StringType(), True),
#         StructField("population", ArrayType(FloatType(), True), True),
#         StructField("continent", StringType(), True),
#         StructField("case_cum", ArrayType(IntegerType(), True), True),
#         StructField("deaths_cum", ArrayType(IntegerType(), True), True),
#     ]
# )

# df_with_schema = spark.read.schema(schema).json("covid.json")

# df_with_schema.show()
# df_with_schema.printSchema()

schema = StructType(
    [
        StructField("date", ArrayType(DateType(), True), True),
        StructField("day", ArrayType(IntegerType(), True), True),
        StructField("month", ArrayType(IntegerType(), True), True),
        StructField("year", ArrayType(IntegerType(), True), True),
        StructField("cases", ArrayType(LongType(), True), True),
        StructField("deaths", ArrayType(IntegerType(), True), True),
        StructField("country", ArrayType(StringType(), True), True),
        StructField("code", ArrayType(StringType(), True), True),
        StructField("population", ArrayType(DoubleType(), True), True),
        StructField("continent", ArrayType(StringType(), True), True),
        StructField("cases_cum", ArrayType(IntegerType(), True), True),
        StructField("deaths_cum", ArrayType(IntegerType(), True), True),
    ]
)

covid_df = spark.read.schema(schema).json("covid.json")

covid_df.printSchema()
covid_df.show()

covid_df_cleaned = covid_df.withColumn(
    "Filtered_Col", F.expr("filter(month ,x -> x <= 12)")
)

covid_df_cleaned.show()

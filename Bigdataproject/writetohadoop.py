from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import max
import time
import os
import json
import requests
import pandas as pd
from datetime import datetime
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read JSON and Write to CSV") \
    .getOrCreate()

# Read JSON file into DataFrame
path = "/home/pratik/DBDA/MyProjects/api_data/2024-02-14.json"

schemadf = "incident_datetime TIMESTAMP ,incident_day_of_week STRING , report_datetime TIMESTAMP , incident_id STRING ,report_type_code STRING , \
incident_category STRING , incident_subcategory STRING , resolution STRING , police_district STRING  , latitude STRING , longitude STRING"
json_df = spark.read.schema(schemadf).option('multiline' , True).\
    option('infer_schema' , True).json(path)

# json_df.describe()
# json_df.show()
# select required columns
df1 = json_df.select("incident_datetime", "incident_day_of_week", \
                             "report_datetime","incident_id","report_type_code","incident_category","incident_subcategory",\
                             "resolution" ,"police_district"  , \
                             "latitude" , "longitude")
df2 =   df1.dropDuplicates()
df3 = df2.withColumnRenamed('incident_day_of_week'  , 'day')

df3.write.format("orc") \
.mode('overwrite').save("hdfs://localhost:9000/user/project/output")
print('successful')

df3.printSchema()

# Stop SparkSession
spark.stop()

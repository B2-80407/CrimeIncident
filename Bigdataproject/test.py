# from datetime import datetime, timedelta
#
#
# current_date = datetime.now()
# original_date_str = current_date.strftime('%Y-%m-%d')
# print(original_date_str)
# i = 1
# while i < 366:
#
#     # Convert string to datetime object
#     original_date = datetime.strptime(original_date_str, '%Y-%m-%d')
#
#     # Calculate one day before
#     day_before = original_date - timedelta(days=1)
#
#     # Convert back to string
#     day_before_str = day_before.strftime('%Y-%m-%d')
#     print(day_before_str)
#     original_date_str = day_before_str
#     i +=1
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ReadORCExample") \
    .getOrCreate()
schemadf = "incident_datetime TIMESTAMP ,day STRING , report_datetime TIMESTAMP , incident_id INT ,report_type_code STRING , \
                   incident_category STRING , incident_subcategory STRING , resolution STRING , police_district STRING  , latitude DOUBLE , longitude DOUBLE"

# Read ORC file
orc_df = spark.read.schema(schemadf).orc("hdfs://localhost:9000/user/project/output")

# Show DataFrame schema and some rows
orc_df.printSchema()
orc_df.show()

# Stop SparkSession
spark.stop()

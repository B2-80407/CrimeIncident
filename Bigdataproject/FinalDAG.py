from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import time
import os
import os
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def save_data(folder_name, api_url):
    current_date = datetime.now() - timedelta(days=2)
    date_str = current_date.strftime('%Y-%m-%d')

    response = requests.get(api_url, params={"incident_datetime": date_str})
    if response.status_code == 200:
        if len(response.text) >= 1:
            file_name = os.path.join(folder_name, f"{date_str}.json")
            with open(file_name, "w") as file:
                file.write(response.text)
            print(f"Data saved for {date_str}")
        else:
            print(f"No data found for {date_str} at {api_url}")
    else:
        print(f"Failed to fetch data for {date_str}")



# data sample
	#[{"incident_datetime":"2024-02-15T00:00:00.000","incident_date":"2024-02-15T00:00:00.000","incident_time":"00:00","incident_year":"2024","incident_day_of_week":"Thursday","report_datetime":"2024-02-15T07:09:00.000","row_id":"136484905151","incident_id":"1364849","incident_number":"240101157","cad_number":"240460461","report_type_code":"II","report_type_description":"Initial","incident_code":"05151","incident_category":"Burglary","incident_subcategory":"Burglary - Other","incident_description":"Burglary, Non-residential, Forcible Entry","resolution":"Open or Active","intersection":"VALLEJO ST \\ POLK ST","cnn":"25317000","police_district":"Northern","analysis_neighborhood":"Russian Hill","supervisor_district":"3","supervisor_district_2012":"3","latitude":"37.796897888183594","longitude":"-122.42195892333984","point":{"type":"Point","coordinates":[-122.42195892333984,37.796897888183594]}}]
	
def etl_process():
    folder_name = "/home/pratik/DBDA/MyProjects/api_data"
    api_url = "https://data.sfgov.org/resource/wg3w-h783.json"

    save_data(folder_name, api_url)

    spark = SparkSession.builder \
        .appName("DumpORC") \
        .getOrCreate()

    path = "/home/pratik/DBDA/MyProjects/api_data/"

    json_df = spark.read \
        .option('inferSchema', True) \
        .option('multiline', True) \
        .json(path)

    df1 = json_df.select("incident_datetime", "incident_day_of_week", \
                         "report_datetime", "incident_id", "report_type_code", "incident_category",
                         "incident_subcategory", \
                         "resolution", "police_district", \
                         "latitude", "longitude")

    df2 = df1.dropDuplicates()
    df3 = df2.withColumnRenamed('incident_day_of_week', 'day')

    df3.write.format("orc") \
        .mode('overwrite').save("hdfs://localhost:9000/user/project/inputdata/data/api_data")
    print('Successful')

    df3.printSchema()
    
def build_latest():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import year, current_date

    # creating a spark session
    spark = SparkSession.builder \
        .appName("build_latest") \
        .getOrCreate()

    # build the latest table
    schemadf = "incident_datetime TIMESTAMP ,day STRING , report_datetime TIMESTAMP , incident_id INT ,report_type_code STRING , \
                       incident_category STRING , incident_subcategory STRING , resolution STRING , police_district STRING  , latitude DOUBLE , longitude DOUBLE"

    # Read ORC file
    orc_df = spark.read.schema(schemadf).orc("hdfs://localhost:9000/user/bigdata/project/inputdata")

    # Show DataFrame schema and some rows
    # orc_df.printSchema()
    # orc_df.show()
    df1 = orc_df.filter(orc_df['resolution'] == 'Open or Active')

    # dump transformed data to HDFS 
    df1.repartition(1) \
        .write.mode('overwrite') \
        .option("header", "true") \
        .format('csv') \
        .save("hdfs://localhost:9000/user/project/output")
    print('latest table HDFS for cluster dump success...')

    # dump  transformed data to local directory for visualization purpose
    df1.repartition(1) \
        .write.mode('overwrite') \
        .option("header", "true") \
        .format('csv') \
        .save("/home/pratik/DBDA/MyProjects/Incidentdata")
    print(f'latest table local dump success...')
    
    # dump  transformed data to mysql database for Analysis purpose
    


dag_arg = {
    'owner': 'Pratik Doiphode',
    'retries': '3',
    'retry_delay': timedelta(minutes=2)
}



# Construct the DAG
with DAG(
     dag_id='Incident7',
     default_args=dag_arg,
     schedule_interval='@once',
     start_date=datetime(2024, 2, 26),
     catchup=True
) as dag:
# with DAG(
#         dag_id='Incident',
#         default_args=dag_arg,
#         schedule_interval='@daily',
#         start_date=datetime(2024, 2, 10),
#         catchup=False
# ) as dag:
    dfs_start = BashOperator(
        task_id='start_dfs',
        bash_command="start-dfs.sh "
    )

    extract_transform_load = PythonOperator(
        task_id='extract_transform_load',
        python_callable=etl_process
    )

    delete_local_data = BashOperator(
        task_id='delete_local_data',
        bash_command='echo second stage running....'
        #bash_command="rm -rf /home/pratik/DBDA/MyProjects/api_data/* "
    )

    wait_for_safemode = BashOperator(
        task_id='wait_for_safemode',
        bash_command="""#! /bin/bash

        check_safemode(){
            hdfs dfsadmin -safemode get | grep "Safe mode is ON"
        }

        while check_safemode; do
            echo "Waiting for NameNode to Come Out of SafeMode. Sleeping for 10 seconds........"
            sleep 20
        done

        echo "NameNode come out of SafeMode.."
        """
    )

    build_latest_table = PythonOperator(
        task_id='build_latest_table',
        python_callable=build_latest
    )

    dfs_stop = BashOperator(
        task_id='stop_dfs',
        bash_command="stop-dfs.sh "
    )

dfs_start >> extract_transform_load >> delete_local_data >> wait_for_safemode >> build_latest_table >> dfs_stop


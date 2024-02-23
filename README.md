# CrimeIncident
A Big Data pipeline for the getting data related to SF Police Department Incident Reports 

-- https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783/data_preview ==> API website

Data Engineering Project using PySpark, Airflow, and Hadoop
Overview:

This project is a comprehensive data engineering solution that fetches data from an external API in JSON format, processes it using PySpark, and stores it locally and on Hadoop for further analysis and visualization. It utilizes Apache Airflow for task scheduling, ensuring automation and reliability.
Project Flow:

    Data Extraction and Transformation:
        The project starts with extracting data from the San Francisco government's data API (https://data.sfgov.org/resource/wg3w-h783.json). The data is filtered based on the incident date, typically two days prior to the current date.
        The extracted JSON data is saved locally in a specified folder.
        PySpark is then used to process the JSON data:
            The JSON data is read into a PySpark DataFrame.
            Irrelevant columns are dropped, and duplicate records are removed.
            The DataFrame schema is modified to suit the requirements.
            Processed data is saved as ORC (Optimized Row Columnar) format on Hadoop.

    Building Latest Table:
        Another PySpark job is executed to build a 'latest' table from the processed data.
        The table includes only open or active incidents, filtering out closed ones.
        The latest table is saved both locally and on Hadoop.

    Airflow DAG Setup:
        An Airflow Directed Acyclic Graph (DAG) orchestrates the project tasks.
        Tasks are configured to execute sequentially:
            Starting and stopping Hadoop Distributed File System (HDFS).
            Extracting, transforming, and loading (ETL) data using PySpark.
            Deleting local data.
            Waiting for Hadoop NameNode to exit safe mode.
            Building the latest table.

    Scheduling:
        The DAG is scheduled to run daily at 12:00 AM, ensuring regular data updates and processing.
        Retries are configured to handle any task failures, with a delay of two minutes between retries.
        
        
Project Components:

    Apache Airflow: Used for workflow automation and scheduling tasks.
    PySpark: Utilized for data processing, transformation, and building data pipelines.
    Hadoop: Acts as a distributed storage system for storing processed data.
    API: Data is fetched from the San Francisco government's data API.
    Bash Commands: Used for starting and stopping HDFS, and waiting for safe mode exit.
    
Dependencies:

    Python (requests, datetime)
    Apache Airflow
    PySpark
    Hadoop

Usage:

    Clone the repository and configure Airflow with the provided DAG.
    Ensure dependencies are installed and properly configured.
    Schedule the DAG to run daily for automated data processing.

Conclusion:

This data engineering project demonstrates an end-to-end solution for fetching, processing, and storing data using PySpark, Airflow, and Hadoop. It provides a scalable and automated approach for handling large datasets and enables seamless data analysis and visualization.

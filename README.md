https://github.com/hannakarpukhina/amazon_movie_ratings/assets/83240320/b8bc770a-d384-4b38-b250-66b7afb2f23a


This project is designed to download movies and TV ratings data and metadata from the Amazon site, store them in Google Cloud Storage, and then process and migrate the data using PySpark into a PostgreSQL database.

Airflow DAG - Data Processing Workflow
This project includes an Apache Airflow DAG for orchestrating the entire data processing workflow. The DAG consists of the following tasks

Prerequisites
Before you begin, ensure you have the following installed:

PySpark
PostgreSQL
Google Cloud SDK
Python 3.x

1. Download Ratings Data Files<br />
Task ID: download_rating_files<br />
Description: Downloads the ratings file from the Amazon site and uploads it to Google Cloud Storage.
2. Download Metadata Files<br />
Task ID: download_metadata_files<br />
Description: Downloads the ratings file from the Amazon site and uploads it to Google Cloud Storage.
3. Create Cluster in GCP Composer<br />
Task ID: create_cluster<br />
Description: Creates a cluster in Google Cloud Composer to execute PySpark scripts.
4. Aggregate Rating Data and Insert into PostgreSQL<br />
Task ID: spark_submit_ratings_job<br />
Description: Uses PySpark to transform and aggregate the rating data from the CSV file and inserts it into the PostgreSQL database.
5. Transform metadata and Insert into PostgreSQL<br />
Task ID: spark_submit_metadata_job<br />
Description: Uses PySpark to transform the metadata from the json file and inserts it into the PostgreSQL database.
6. Delete Cluster<br />
Task ID: delete_cluster<br />
Description: Deletes the GCP Composer cluster to free up resources when the processing is complete.


Acknowledgments<br />
Thank you to the Apache Airflow community for providing a robust workflow orchestration tool.

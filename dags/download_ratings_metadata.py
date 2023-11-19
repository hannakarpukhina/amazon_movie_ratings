import datetime
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
import scripts.download_to_gcs as dtg
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
AMAZON_URL_BASE = "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles"
EXEC_DATE = "{{ ds }}"
PROJECT_ID = "idyllic-adviser-396909"
CLUSTER_NAME = "cluster-amazon-ratings-m"
DAGS_SCRIPT_PATH = "gs://us-central1-rt-amazon-278529e6-bucket/dags/scripts"
REGION = "us-east4"
JAR_FILE_URIS = "gs://postgresql_confg/postgresql-42.5.4.jar"
TARGET_BUCKET = "amazon_movie_ratings"

default_args = {
    "owner": "Composer Ratings",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}
with airflow.DAG(
    "movies_and_tv_ratings_processing",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    ratings_target_file = f"ratings_{EXEC_DATE}/ratings_Movies_and_TV.csv"
    download_rating_file = PythonOperator(
        task_id="download_rating_file",
        python_callable=dtg.download_to_gcs,
        op_args=[
            f"{AMAZON_URL_BASE}/ratings_Movies_and_TV.csv",
            TARGET_BUCKET,
            ratings_target_file,
        ],
        dag=dag,
    )
    metadata_target_file = f"ratings_{EXEC_DATE}/meta_Movies_and_TV.json.gz"
    download_metadata_file = PythonOperator(
        task_id="download_metadata_file",
        python_callable=dtg.download_to_gcs,
        op_args=[
            f"{AMAZON_URL_BASE}/meta_Movies_and_TV.json.gz",
            TARGET_BUCKET,
            metadata_target_file,
        ],
        dag=dag,
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        num_workers=0,
        master_machine_type="n2-standard-4",
        worker_machine_type="n2-standard-4",
        master_disk_size=200,
    )
    pyspark_job_template = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "jar_file_uris": [JAR_FILE_URIS],
            "file_uris": [
                f"{DAGS_SCRIPT_PATH}/postgres_service.py",
                f"{DAGS_SCRIPT_PATH}/spark_service.py",
                f"{DAGS_SCRIPT_PATH}/rating_constant.py",
                f"{DAGS_SCRIPT_PATH}/check_table_exist.py",
                f"{DAGS_SCRIPT_PATH}/metadata_constant.py",
            ],
        },
    }
    pyspark_ratings_job = pyspark_job_template.copy()
    pyspark_ratings_job["pyspark_job"][
        "main_python_file_uri"
    ] = f"{DAGS_SCRIPT_PATH}/process_rating.py"
    pyspark_ratings_job["pyspark_job"]["args"] = [
        f"gs://{TARGET_BUCKET}/{ratings_target_file}",
        "csv",
    ]

    spark_submit_ratings_job = DataprocSubmitJobOperator(
        task_id="spark_submit_ratings_job",
        job=pyspark_ratings_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
    pyspark_metadata_job = pyspark_job_template.copy()
    pyspark_metadata_job["pyspark_job"][
        "main_python_file_uri"
    ] = "{DAGS_SCRIPT_PATH}/process_metadata.py"
    pyspark_metadata_job["pyspark_job"]["args"] = [
        f"gs://{TARGET_BUCKET}/{metadata_target_file}",
        "json",
    ]

    spark_submit_metadata_job = DataprocSubmitJobOperator(
        task_id="spark_submit_metadata_job",
        job=pyspark_metadata_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

[download_rating_file, download_metadata_file] >> create_cluster
(
    create_cluster
    >> [spark_submit_ratings_job, spark_submit_metadata_job]
    >> delete_cluster
)

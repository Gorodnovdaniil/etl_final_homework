import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Данные вашей инфраструктуры
YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDKkb/MDUx3DRCxwU0UPk7FNGu7LrNbhLdQ5R/H8N92UJ70u5t68iApdGJqXmy6d4z5WgGOaHdAnSWqEIP4lCCXacgylRvW1hZhEFaS2TAxGXxiMXo5X9vDxfFcp/d/oynIVCB0hKnX3rQOd9dQH6jzapLUTioUaUDoVN02t8NOX+zUVsuTvSuczbS9vGlG1LHMVJ37PPhHzrbwzCnJfTkndNb25k/QJdEYSZh57VJvPg8Hnve/wFVHxYBvlrj0TkfabxFECgqSevNenCkmysWgWnfRkhCDEtwBQz6SgxYLNuQIHQnc3W4mmlpGrOG7Mp+ktKDczcGI+jYAP/EKOncp'
YC_DP_SUBNET_ID = 'e9bpqv9p8s5suacls7as'
YC_DP_SA_ID = 'ajeh1d13r3jomfaa83fp'
YC_DP_METASTORE_URI = '10.128.0.18'
YC_BUCKET = 'gorodokbucket'

with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Temporary cluster for PySpark job',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.0',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=20,
        computenode_count=1,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster

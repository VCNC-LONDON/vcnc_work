import os 
from datetime import datetime
from dependencies import utils
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.python_operator import PythonOperator


yesterday = "{{ macros.ds_add(ds, -0) }}"
yesterday_suffix = "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"

with models.DAG(
        dag_id='sql_branch_example',
        description='sql_branch_example',
        schedule_interval='0 1 * * *',
        start_date=datetime(2022, 7, 15),
    ) as dag:

    check_origin_table_update = BigQueryTableExistenceSensor(
        task_id="check_origin_table_update", 
        project_id={project_id}, 
        dataset_id={dataset}, 
        table_id={table_name},
        gcp_conn_id={your_conn_key},     
    )

    check_table_level_quality_of_origin_table = BigQueryValueCheckOperator(
        task_id="check_table_level_quality_of_origin_table",
        sql=f"SELECT COUNT(DISTINCT type) FROM `project_id.dataset_id.table_name` WHERE date_kr = {yesterday}",
        pass_value=1,
        use_legacy_sql=False,
    )

    check_target_table_exsits = BranchSQLOperator(
        task_id = "check_target_table_exsits",
        conn_id = {your_conn_key},
        sql = f'SELECT IF(COUNT(1) > 0, True, False) FROM `project_id.dataset_id.target_table_name` WHERE date_kr = "{yesterday}"',
        follow_task_ids_if_true = "pass_update_target_table",
        follow_task_ids_if_false = "update_target_table_task",
        parameters={"use_legacy_sql":False} #optional
    )

    pass_update_target_table = PythonOperator(
        task_id='pass_update_target_table',
        python_callable = pass_update_table_data_callable,
    )

    update_target_table_task = BigQueryOperator(
        task_id=f"update_target_table_task",
        sql=utils.read_sql(os.path.join(os.environ['DAGS_FOLDER'], 'sql/example.sql')).format(execute_date=yesterday_suffix),
        bigquery_conn_id={your_conn_key},
        use_legacy_sql=False,
        destination_dataset_table=f'project_id.dataset_id.target_table_name${yesterday_suffix}',
        write_disposition='WRITE_TRUNCATE',
        time_partitioning={'type': 'DAY', 'field': 'date_kr'},
    )

    check_origin_table_update >> check_table_level_quality_of_origin_table >> check_target_table_exsits >> [update_target_table_task, pass_update_target_table]
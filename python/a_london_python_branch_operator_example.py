# BranchPythonOperator(python_callable, op_args, op_kwargs)

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from dependencies.utils import get_airflow_dev_env # 현재 동작중인 airflow 환경을 가져오는 custom function 
from datetime import datetime

def branch_callable(env):
	if env == "production" :
		return "production_task"
	else :
		return "staging_task"


env = get_airflow_dev_env()

with DAG(
    dag_id='example_BranchPythonOperator',
    start_date=datetime(2022, 7, 15),
    schedule_interval= '* * * * *'
    ) as dag:
    
    branch_task = BranchPythonOperator(
        task_id = "env_branching",
        python_callable = branch_callable,
        op_kwargs = {"env" : env}
	)
    
    production_task = DummyOperator(
		task_id = "production_task"
    )
    
    staging_task = DummyOperator(
		task_id = "staging_task"
    )
    
    branch_task >> [production_task, staging_task]
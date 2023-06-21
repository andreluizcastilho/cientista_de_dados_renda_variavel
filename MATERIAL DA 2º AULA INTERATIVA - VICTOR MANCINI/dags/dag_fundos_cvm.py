import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


default_args = {
    "owner": "local",
    "start_date": days_ago(1)
}

dag = DAG(
    "dag_fundos_cvm", 
    default_args=default_args,
    schedule_interval='0 21 * * FRI', #https://crontab.guru/
)

# Task 1
dummy_task_comeco = DummyOperator(
    task_id='dummy_task_comeco',
    dag=dag
)

# Task 2
run_fundos_cvm = BashOperator(
    task_id='run_fundos_cvm', 
    bash_command=f"cd xp_crv && python3 -m cvm_fundos_acoes.cli run_fundos_cvm",
    dag=dag
)

# Task 3
run_download_fundos_cvm = BashOperator(
    task_id='run_download_fundos_cvm', 
    bash_command="cd xp_crv && python3 -m cvm_fundos_acoes.cli run_download_fundos_cvm",
    dag=dag,
    trigger_rule="all_success"
)

# Task 4
dummy_task_fim = DummyOperator(
    task_id='dummy_task_fim',
    dag=dag
)

# Workflow
dummy_task_comeco >> run_fundos_cvm >> run_download_fundos_cvm >> dummy_task_fim
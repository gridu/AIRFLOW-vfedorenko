import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

_current_dir = os.path.dirname(__file__)
_parent_dir = os.path.abspath(os.path.join(_current_dir, os.path.pardir))

default_args = {
    'owner': 'Vadim Fedorenko',
    'schedule_interval': None,
    'start_date': datetime(2019, 11, 30),
}

_triggers_dir = os.path.join(_parent_dir, 'triggers')
_trigger_file = os.path.join(_triggers_dir, 'run')
_trigger_dag_id = 'table_1_extractor'


def _file_sensor_op() -> FileSensor:
    return FileSensor(task_id='wait_trigger_file', filepath=_trigger_file)


def _trigger_dag_op() -> TriggerDagRunOperator:
    return TriggerDagRunOperator(task_id='trigger_dag', trigger_dag_id=_trigger_dag_id)


def _remove_trigger_file_op() -> BashOperator:
    return BashOperator(task_id='remove_trigger_file', bash_command=f'rm {_trigger_file}')


dag_id = 'trigger_dag'
with DAG(dag_id, default_args=default_args) as dag:
    _file_sensor_op() >> _trigger_dag_op() >> _remove_trigger_file_op()
    globals()[dag_id] = dag

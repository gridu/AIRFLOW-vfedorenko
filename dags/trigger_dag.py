import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator

_current_dir = os.path.dirname(__file__)

default_args = {
    'owner': 'Vadim Fedorenko',
    'schedule_interval': None,
    'start_date': datetime(2019, 11, 30),
}

_triggers_dir = os.path.join(_current_dir, 'triggers')
_trigger_file = os.path.join(_triggers_dir, 'run')

_finished_files_dir = os.path.join(_current_dir, 'finished')


def _file_sensor() -> FileSensor:
    return FileSensor(task_id='wait_trigger_file', filepath=_trigger_file)


def _trigger_dag_op(trigger_dag_id_, *args, **kwargs) -> TriggerDagRunOperator:
    def __dag_run_obj_fn(context, dro):
        task_instance: TaskInstance = context['ti']
        task_instance.xcom_push(key='trigger_dag_id', value=trigger_dag_id_)
        return dro

    return TriggerDagRunOperator(
        task_id='trigger_dag', trigger_dag_id=trigger_dag_id_, python_callable=__dag_run_obj_fn,
        *args, **kwargs
    )


def _process_results_sub_dag_op(parent_dag_id_, trigger_dag_id_) -> SubDagOperator:
    def _triggered_dag_sensor() -> ExternalTaskSensor:
        # noinspection PyTypeChecker
        return ExternalTaskSensor(
            task_id='sensor_triggered_tag',
            external_dag_id=trigger_dag_id_,
            external_task_id=None,
            allowed_states=['success'],
            check_existence=True
        )

    def _print_result_op(*args, **kwargs) -> PythonOperator:
        def __callable_op(task_instance: TaskInstance, **context):
            import logging
            target_dag_status = task_instance.xcom_pull(dag_id=trigger_dag_id_, key='dag_status')
            logging.getLogger(__file__).info(f'Target DAG status: {target_dag_status}')

        return PythonOperator(
            task_id='print_result', python_callable=__callable_op, provide_context=True,
            *args, **kwargs
        )

    def _remove_trigger_file_op(*args, **kwargs) -> BashOperator:
        return BashOperator(task_id='remove_trigger_file', bash_command=f'rm {_trigger_file}', *args, **kwargs)

    def _create_finished_ts(*args, **kwargs) -> BashOperator:
        return BashOperator(
            task_id='create_finished_timestamp',
            bash_command='touch %s/finished_{{ ds_nodash }}' % _finished_files_dir,
            *args, **kwargs
        )

    sub_dag = DAG(f'{parent_dag_id_}.process_results', default_args=default_args, schedule_interval=None)

    with sub_dag:
        # @formatter:off
        _triggered_dag_sensor() \
            >> _print_result_op() \
            >> _remove_trigger_file_op() \
            >> _create_finished_ts()
        # @formatter:on

    return SubDagOperator(task_id='process_results', subdag=sub_dag)


dag_id = 'trigger_dag'
with DAG(dag_id, default_args=default_args, schedule_interval=None) as dag:
    trigger_dag_id = Variable.get('dag_id', 'users_table_processor')

    # @formatter:off
    _file_sensor() \
        >> _trigger_dag_op(trigger_dag_id_=trigger_dag_id, execution_date='{{execution_date}}') \
        >> _process_results_sub_dag_op(parent_dag_id_=dag_id, trigger_dag_id_=trigger_dag_id)
    # @formatter:on

    globals()[dag_id] = dag

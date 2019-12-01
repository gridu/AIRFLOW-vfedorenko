import logging
import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'Vadim Fedorenko'
}

config = {
    'table_1_extractor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'table_1'
    },
    'table_2_processor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'table_2'
    },
    'table_3_postprocessor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'table_3'
    }
}


def _print_process_start_op() -> PythonOperator:
    def __callable_fn():
        logger = logging.getLogger(__file__)
        logger.info('{dag_id} start processing tables in database: {database}')

    return PythonOperator(task_id='print_process_start', python_callable=__callable_fn)


def _get_current_user_op() -> BashOperator:
    return BashOperator(task_id='get_current_user', bash_command='echo $USER')


def _skip_table_creation_op(*args, **kwargs) -> DummyOperator:
    return DummyOperator(task_id='skip_table_creation', *args, **kwargs)


def _create_table_op(*args, **kwargs) -> DummyOperator:
    return DummyOperator(task_id='create_table', *args, **kwargs)


def _check_table_exists_branch_op(*args, **kwargs) -> BranchPythonOperator:
    def __callable_op() -> str:
        """ method to check that table exists """
        if random.randint(0, 10) > 5:
            return 'skip_table_creation'
        else:
            return 'create_table'

    return BranchPythonOperator(task_id='check_table_exist', python_callable=__callable_op, *args, **kwargs)


def _insert_new_row_op(*args, **kwargs) -> DummyOperator:
    return DummyOperator(task_id='insert_new_row', *args, **kwargs)


def _query_the_table_op(*args, **kwargs) -> DummyOperator:
    return DummyOperator(task_id='query_the_table', *args, **kwargs)


for dag_id, dag_config in config.items():
    with DAG(dag_id, default_args={**default_args, **dag_config}) as dag:
        globals()[dag_id] = dag

        # @formatter:off
        _print_process_start_op() \
            >> _get_current_user_op() \
            >> _check_table_exists_branch_op() >> [_create_table_op(), _skip_table_creation_op()] \
            >> _insert_new_row_op(trigger_rule='none_failed') \
            >> _query_the_table_op()
        # @formatter:on

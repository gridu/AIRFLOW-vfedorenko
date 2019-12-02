import logging
from datetime import datetime
from uuid import uuid4

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# noinspection PyUnresolvedReferences
from airflow.operators.postgres_count_rows_plugin import PostgresCountRowsOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'Vadim Fedorenko'
}

database = 'PostgreSQL'

config = {
    'users_table_processor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'users_table'
    },
    'products_table_processor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'products_table'
    },
    'accounts_table_processor': {
        'schedule_interval': None,
        'start_date': datetime(2019, 11, 30),
        'table_name': 'accounts_table'
    }
}


def _print_process_start_op() -> PythonOperator:
    def __callable_fn(task_instance: TaskInstance, **context):
        logger = logging.getLogger(__file__)
        logger.info(f'{task_instance.dag_id} started processing tables in database {database}')

    return PythonOperator(task_id='print_process_start', python_callable=__callable_fn, provide_context=True)


def _get_current_user_op() -> BashOperator:
    return BashOperator(
        task_id='get_current_user',
        bash_command='''
        if [[ ! -z "${USER}" ]]; then
            echo "${USER}";
        else
            echo "root"
        fi
        ''',
        xcom_push=True
    )


def _skip_table_creation_op(*args, **kwargs) -> DummyOperator:
    return DummyOperator(task_id='skip_table_creation', *args, **kwargs)


def _create_table_op(table_name, *args, **kwargs) -> PostgresOperator:
    return PostgresOperator(
        task_id=f'create_table_{table_name}',
        sql=f'CREATE TABLE {table_name}('
            f'   custom_id INTEGER NOT NULL, '
            f'   user_name VARCHAR (50) NOT NULL, '
            f'   timestamp TIMESTAMP NOT NULL'
            f');'
    )


def _get_schema_op(*args, **kwargs) -> PythonOperator:
    def __callable_op(task_instance: TaskInstance, **context):
        hook = PostgresHook()

        # get schema name
        schema = None
        query = hook.get_records(sql="SELECT * FROM pg_tables")

        for result in query:
            if 'airflow' in result:
                schema = result[0]
                break

        task_instance.xcom_push(key='schema_name', value=schema)

    return PythonOperator(task_id='get_schema', python_callable=__callable_op, provide_context=True, *args, **kwargs)


def _check_table_exists_branch_op(table_name, *args, **kwargs) -> BranchPythonOperator:
    def __callable_op(task_instance: TaskInstance, **context) -> str:
        """ callable function to get schema name and after that check if table exist """
        hook = PostgresHook()

        schema = task_instance.xcom_pull(task_ids=['get_schema'], key='schema_name')[0]

        # check table exist
        query = hook.get_first(
            sql=f"SELECT * FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
        )

        if query:
            return 'skip_table_creation'
        else:
            return f'create_table_{table_name}'

    return BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=__callable_op,
        provide_context=True,
        *args, **kwargs
    )


def _insert_new_row_op(table_name, *args, **kwargs) -> PostgresOperator:
    unique_id = uuid4().int % 123456789

    return PostgresOperator(
        task_id='insert_new_row',
        # @formatter:off
        sql='''INSERT INTO %s VALUES (
                %s, 
                '{{ ti.xcom_pull(task_ids=["get_current_user"], key="return_value")[0] }}', 
                to_timestamp(%s)
            );''' % (
                table_name,
                unique_id,
                int(datetime.now().timestamp())
            ),
        # @formatter:on
        *args, **kwargs
    )


def _query_table_op(table_name, *args, **kwargs) -> PythonOperator:
    return PostgresCountRowsOperator(task_id='query_table', table_name=table_name, *args, **kwargs)


for dag_id, dag_config in config.items():
    with DAG(dag_id, default_args={**default_args, **dag_config}, schedule_interval=None) as dag:
        table_name_ = config[dag_id]['table_name']

        # @formatter:off
        _print_process_start_op() \
            >> _get_current_user_op() \
            >> _get_schema_op() \
            >> _check_table_exists_branch_op(table_name=table_name_) \
            >> [_create_table_op(table_name=table_name_), _skip_table_creation_op()] \
            >> _insert_new_row_op(table_name=table_name_, trigger_rule='none_failed') \
            >> _query_table_op(table_name=table_name_)
        # @formatter:on

        globals()[dag_id] = dag

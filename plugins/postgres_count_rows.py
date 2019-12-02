from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.plugins_manager import AirflowPlugin


class PostgresCountRowsOperator(PythonOperator):

    def __init__(self, task_id, table_name, xcom_key='return_value', *args, **kwargs):
        def __callable_fn(task_instance: TaskInstance, **context):
            hook = PostgresHook()

            row = hook.get_first(sql=f"SELECT  COUNT(*)  FROM {table_name}")[0]
            task_instance.xcom_push(key=xcom_key, value=row)

        super().__init__(task_id=task_id, python_callable=__callable_fn, provide_context=True, *args, **kwargs)


class PostgresCountRowsPlugin(AirflowPlugin):
    name = 'postgres_count_rows_plugin'
    operators = [PostgresCountRowsOperator]

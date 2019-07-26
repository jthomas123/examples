from airflow.operators.python_operator import PythonOperator
import logging

# see https://stackoverflow.com/questions/41749974/airflow-using-template-files-for-pythonoperator
# for why this is necessary

class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql', '.jinja2')

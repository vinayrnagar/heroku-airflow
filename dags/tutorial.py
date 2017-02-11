"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](http://pythonhosted.org/airflow/tutorial.html)
"""
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
import airflow.hooks.S3_hook
from airflow.operators.python_operator import PythonOperator

ten_days_ago = datetime.combine(datetime.today() - timedelta(10),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': ten_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('db_dump', default_args=default_args)


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='db_dump',
    bash_command='/app/vendor/heroku-toolbelt/bin/heroku pg:psql -c "\copy dag to "/app/dag.csv" csv header" --app airflowtest2',
    dag=dag)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__


def load_to_s3():
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id="s3")
    hook.load_file('dag.csv', 'dag.csv', 'bw-test-vinay', True)

t2 = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    trigger_rule="all_done",
    dag=dag)


# templated_command = """
# {% for i in range(5) %}
#     echo "{{ ds }}"
#     echo "{{ macros.ds_add(ds, 7)}}"
#     echo "{{ params.my_param }}"
# {% endfor %}
# """

# t3 = BashOperator(
#     task_id='templated',
#     depends_on_past=False,
#     bash_command=templated_command,
#     params={'my_param': 'Parameter I passed in'},
#     dag=dag)

t2.set_upstream(t1)
# t3.set_upstream(t1)

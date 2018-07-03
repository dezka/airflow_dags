import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG, Variable

import time
from pprint import pprint

default_args = {
    'owner': 'dezka',
    'start_date': airflow.utils.dates.days_ago(2),
    'url': 'https://www.ssa.gov/oact/babynames/state/namesbystate.zip',
    'output_file': '/tmp/work/namesbystate.zip',
    'dest_dir': '/tmp/work/nbs',
    'howmanyfiles': 5
}

dag = DAG(
    dag_id='baby_names_by_state', default_args=default_args,
    schedule_interval=None)


def download_file(url=default_args['url'], *args, **kwargs):
    import requests
    r = requests.get(url)
    with open(default_args['output_file'], 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def unzip_file(f=default_args['output_file'], dest=default_args['dest_dir'], *args, **kwargs):
    import zipfile
    with zipfile.ZipFile(f, 'r') as zip_ref:
        zip_ref.extractall(dest)


def generate_file(src_dir=default_args['dest_dir'], c=default_args['howmanyfiles'], **kwargs):
    import random
    from os.path import isfile, join
    from os import listdir
    files = [f for f in listdir(src_dir) if f.endswith(".TXT") and isfile(join(src_dir, f))]
    f_list = list()
    for i in range(c):
        f_list.append(src_dir + '/' + random.choice(files))
    return f_list


def get_max(**kwargs):
    import pandas as pd
    import os
    ti = kwargs['ti']
    fi_list = ti.xcom_pull(task_ids='get_a_file')
    max_counts = dict()
    for i in range(len(fi_list)):
        f = fi_list[i]
        df = pd.read_csv(f, sep=",", header=None, names=['state','gender','year','name','count'])
        max_count = df['count'].max()
        basepath = os.path.basename(f)
        max_counts[basepath] = max_count
    return max_counts
    

def get_xcom_ob(**kwargs):
    ti = kwargs['ti']
    fi = ti.xcom_pull(task_ids='get_a_file')
    return "The XCOM object from generate_file is {0}".format(fi)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

# Begin the tasks

t1 = BashOperator(
    task_id='make_dest_dir',
    bash_command='mkdir -p {0}'.format(default_args['dest_dir']),
    dag=dag)

t2 = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=dag)


t3 = PythonOperator(
    task_id='unzip_file',
    python_callable=unzip_file,
    dag=dag)


t4 = PythonOperator(
    task_id='get_a_file',
    provide_context=True,
    python_callable=generate_file,
    dag=dag)


t5 = PythonOperator(
    task_id='find_max',
    provide_context=True,
    python_callable=get_max,
    dag=dag)


t6 = PythonOperator(
    task_id='get_xcom_test',
    provide_context=True,
    python_callable=get_xcom_ob,
    dag=dag)


t7 = PythonOperator(
    task_id='print_kwargs',
    provide_context=True,
    python_callable=print_context,
    dag=dag)


t1 >> t2 >> t3 >> t4 >> t5
t4 >> t6
t4 >> t7

#t6 = BashOperator(
#    task_id='clean_up',
#    bash_command='rm -rf {}'.format(default_args['dest_dir']),
#    dag=dag)

#t6 << t5


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


from airflow.utils.dates import days_ago, infer_time_unit

default_args = {
    'owner': 'Maycon Batestin',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    
}

csv = DAG('MYSQL',default_args=default_args, tags=["pyspark", "dados", "mysql", "pipeline"], description='Pipeline para processamento de dados para o banco de dados MYSQL', schedule_interval=None, catchup=False)


assassinatos = BashOperator(
    task_id='assassinatos',
	bash_command='python /usr/local/airflow/myslq/scripts/assassinatos/init.py',
    dag=csv
)

basquet = BashOperator(
    task_id='nba',
	bash_command='python /usr/local/airflow/myslq/scripts/basquet/init.py',
    dag=csv
)

bebidas = BashOperator(
    task_id='bebidas',
	bash_command='python /usr/local/airflow/myslq/scripts/bebidas/init.py',
    dag=csv
)

casamento = BashOperator(
    task_id='casamento',
	bash_command='python /usr/local/airflow/myslq/scripts/casamento/init.py',
    dag=csv
)

covid = BashOperator(
    task_id='covid',
	bash_command='python /usr/local/airflow/myslq/scripts/covid/init.py',
    dag=csv
)

crimes = BashOperator(
    task_id='crimes',
	bash_command='python /usr/local/airflow/myslq/scripts/crimes/init.py',
    dag=csv
)

drogas = BashOperator(
    task_id='drogas',
	bash_command='python /usr/local/airflow/myslq/scripts/drogas/init.py',
    dag=csv
)

fifa = BashOperator(
    task_id='fifa',
	bash_command='python /usr/local/airflow/myslq/scripts/fifa/init.py',
    dag=csv
)

filmes = BashOperator(
    task_id='filmes',
	bash_command='python /usr/local/airflow/myslq/scripts/filmes/init.py',
    dag=csv
)

harrypotter = BashOperator(
    task_id='harrypotter',
	bash_command='python /usr/local/airflow/myslq/scripts/harrypotter/init.py',
    dag=csv
)

heroes = BashOperator(
    task_id='heroes',
	bash_command='python /usr/local/airflow/myslq/scripts/heroes/init.py',
    dag=csv
)

madmen = BashOperator(
    task_id='madmen',
	bash_command='python /usr/local/airflow/myslq/scripts/madmen/init.py',
    dag=csv
)

olimpiadas = BashOperator(
    task_id='olimpiadas',
	bash_command='python /usr/local/airflow/myslq/scripts/olimpiadas/init.py',
    dag=csv
)

senhordosaneis = BashOperator(
    task_id='senhordosaneis',
	bash_command='python /usr/local/airflow/myslq/scripts/senhordosaneis/init.py',
    dag=csv
)

simpsons = BashOperator(
    task_id='simpsons',
	bash_command='python /usr/local/airflow/myslq/scripts/simpsons/init.py',
    dag=csv
)

tarantino = BashOperator(
    task_id='tarantino',
	bash_command='python /usr/local/airflow/myslq/scripts/tarantino/init.py',
    dag=csv
)

assassinatos >> basquet >> bebidas >> casamento >> covid >> crimes >> drogas >> fifa >> filmes >> harrypotter >> heroes >> madmen >> olimpiadas >> senhordosaneis >> simpsons >> tarantino
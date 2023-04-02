from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
import requests
# import pymssql
import pyodbc
# import os

default_args = {
    'owner': 'sehrish',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='lush_my_dag',
    default_args=default_args,
    description='Scrape and store data from LUSH website',
    schedule_interval=timedelta(days=1),
)

# os.environ['PYODBC_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'

def scrape_data():
    # Scrape data from website
    url = 'https://www.lushusa.com/bath/bath-bombs/'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    data = soup.prettify()
    return data


def store_data():
    # Store scraped data in SQL Server
    conn_str = "DRIVER=SQL Server;SERVER=DESKTOP-QI6H2EA\HS;DATABASE=Flow;UID=sa;PWD=abc123"
    cnxn = pyodbc.connect(conn_str)
 
    cursor = cnxn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS LushData (ID INT IDENTITY(1,1), Data VARCHAR(MAX));')
    cursor.execute('INSERT INTO LushData (Data) VALUES (?)', scrape_data())
    cnxn.commit()


def display_data():
    # Display scraped data
    server = Variable.get('sql_server')
    database = Variable.get('sql_database')
    username = Variable.get('sql_username')
    password = Variable.get('sql_password')
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute('SELECT * FROM LushData;')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

t1 = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='display_data',
    python_callable=display_data,
    dag=dag
)

# t1
t1 >> t2 >> t3

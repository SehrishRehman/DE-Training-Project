from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import csv
import requests
from bs4 import BeautifulSoup


def scrape_data():
    """
    Function to scrape data from the specified URL.
    """
    url = 'https://www.lushusa.com/bath/bath-bombs/'

    # Send an HTTP request to the URL of the webpage you want to access
    response = requests.get(url)

    # Parse the content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the elements on the page that contain the data you want to scrape
    products = soup.find_all("div", class_="product-tile")

    # Create an empty list to store the data
    data = []

    # Loop through the products and extract the data
    for product in products:
        name_elem = product.find("div", class_="product-tile__title")
        price_elem = product.find("div", class_="product-tile__price")
        if name_elem and price_elem:
            name = name_elem.text.strip()
            price = price_elem.text.strip()
            data.append({"name": name, "price": price})

    # Create a pandas dataframe from the data
    df = pd.DataFrame(data)

    # Convert the dataframe to a dictionary and return it
    return df.to_dict(orient="records")




def save_to_excel(ds, **kwargs):
    """
    Function to save scraped data to an excel file.
    """
    scraped_data = kwargs['ti'].xcom_pull(task_ids='scrape_task')

    # Create a pandas DataFrame from the scraped data
    df = pd.DataFrame(scraped_data)

    # Save the DataFrame to an excel file
    df.to_excel('scraped_data.xlsx', index=False)


def display_data():
    with open('scraped_data.xlsx', 'r') as file:
        # reader = csv.reader(file)
        reader  = csv.reader('scraped_data.xlsx', encoding= 'unicode_escape')
        for row in reader:
            print(row)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lush',
    default_args=default_args,
    description='A DAG to scrape data from the Lush USA website',
    schedule_interval='@daily',
)



# Task to scrape data from the Lush USA website
scrape_task = PythonOperator(
    task_id='scrape_task',
    python_callable=scrape_data,
    dag=dag,
)

# Task to save the scraped data to an xlsx file
save_task = PythonOperator(
    task_id='save_task',
    provide_context=True,
    python_callable=save_to_excel,
    dag=dag,
)

display_task = PythonOperator(
    task_id='display_data',
    python_callable=display_data,
    dag=dag
)
# Set task dependencies
# save_task.set_upstream(scrape_task)
# Set the dependencies for the tasks
save_task.set_upstream(scrape_task)
display_task.set_upstream(save_task)

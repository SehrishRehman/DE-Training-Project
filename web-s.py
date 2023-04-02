from bs4 import BeautifulSoup
from selectorlib import Extractor
import requests
import pandas as pd 
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# from collections.abc import MutableMapping
# import collections.abc



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
    dag_id='lush',
    default_args=default_args,
    description='A DAG to scrape data from the Lush USA website',
    schedule_interval=timedelta(days=1),
)



def scrape_data():
    r = requests.get('https://www.lushusa.com/bath/bath-bombs/?cgid=bath-bombs&start=0&sz=28')

    # Parse the content of the page using BeautifulSoup
    soup = BeautifulSoup(r.content, "html.parser")

    # Find the elements on the page that contain the data you want to scrape
    products = soup.find_all("div", class_="product-tile")

    # Create an empty list to store the data
    data = []

    # Loop through the products and extract the data
    for product in products:
        name_elem = product.find("div", class_="h3.product-tile-name")
        price_elem = product.find("div", class_="div.tile-price-size span.tile-price")
        if name_elem and price_elem:
            name = name_elem.text.strip()
            price = price_elem.text.strip()
            data.append({"name": name, "price": price})

    return data


def save_data(ds, **kwargs):
    scraped_data = kwargs['ti'].xcom_pull(task_ids='scrape_data')

    # Create a pandas DataFrame from the scraped data
    df = pd.DataFrame(scraped_data)

    # Save the DataFrame to a CSV file
    df.to_csv('products.csv', index=False, encoding='utf-8')


def display_data():
    with open('products.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)


# Task to scrape data from the Lush USA website
scrape_data= PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag,
)

# Task to save the scraped data to an xlsx file
save_data = PythonOperator(
    task_id='save_data',
    provide_context=True,
    python_callable=save_data,
    dag=dag,
)

display_data = PythonOperator(
    task_id='display_data',
    python_callable=display_data,
    dag=dag
)
# Set task dependencies
# save_task.set_upstream(scrape_task)
# Set the dependencies for the tasks
save_data.set_upstream(scrape_data)
display_data.set_upstream(save_data)















# from selenium import webdriver
# from bs4 import BeautifulSoup
# from selectorlib import Extractor
# from selenium.webdriver.chrome.service import Service
# import pandas as pd

# # driver = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")


# service = Service('/usr/lib/chromium-browser/chromedriver')
# driver = webdriver.Chrome(service=service)


# products=[] #List to store name of the product
# prices=[] #List to store price of the product
# taglines=[] #List to store rating of the product
# driver.get("https://www.lushusa.com/bath/bath-bombs/")

# content = driver.page_source
# soup = BeautifulSoup(content)
# for a in soup.findAll('a',href=True, attrs={'class':'row product-grid'}):
#     # name=a.find('div', attrs={'class':'product-tile-category w-100 text-uppercase tiny-font-size letter-spacing-sm empress text-center'})
#     name=a.find('div', attrs={'class':'product-id'})
#     price=a.find('div', attrs={'class':'tile-price-size my-1 text-center  single'})
#     tagline=a.find('div', attrs={'class':'product-tile-tagline d-none black mb-2 text-center smaller-font-size d-md-block'})
#     products.append(name.text)
#     prices.append(price.text)
#     taglines.append(tagline.text)


# df = pd.DataFrame({'Product Name':products,'Price':prices,'Tagline':taglines}) 
# df.to_csv('products.csv', index=False, encoding='utf-8')
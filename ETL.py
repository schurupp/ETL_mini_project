#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3


# In[2]:


def log_progress(message):
    time_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


# In[3]:


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")
    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {"Name": col[1].find_all("a")[1].contents[0],
                        "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index = True)
    return df


# In[4]:


def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path).set_index("Currency").to_dict()["Rate"]
    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]
    
    return df


# In[5]:


def load_to_csv(df, output_path):
    df.to_csv(output_path)
    
def load_to_db(df, sql_conn, table_name):
    df.to_sql(table_name, sql_conn, if_exists = "replace", index = False)


# In[7]:


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './Largest_banks.csv'
exchange_rate_path = "./exchange_rate.csv"
table_name = "Largest Banks"
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate_path)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
log_progress('Process Complete.')
sql_connection.close()


# In[ ]:





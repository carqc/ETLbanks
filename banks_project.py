# Code for ETL operations on Country-GDP data
# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ',' + message + '\n')



def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    #Cargar página web como HTML
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # the variable tables gets the body of all the tables in the web page and the variable rows gets all the rows of the first table.
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Name": col[1].contents[2].contents[0],
                         "MC_USD_Billion": float((col[2].contents[0]).replace("\n", ""))}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = extract_from_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency')['Rate'].to_dict()

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('GBP', 0), 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('EUR', 0), 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate.get('INR', 0), 2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name = 'Largest_banks'
table_attribs = ["Name","MC_USD_Billion"]
output_path = './Largest_banks_data.csv'
db_path = '/home/project/Practice_Proy/Banks.db'
db_name = 'Banks.db'
csv_path = '/home/project/FinalProy/exchange_rate.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('	Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
log_progress('Process Complete')

query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
log_progress('Process Complete')

query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import requests
from datetime import datetime
import numpy as np

log_file = 'logs_fp.txt'
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
df = pd.DataFrame(columns=['Rank','Bank Name','MC_USD_Billion'])
table_name = 'Largest_banks'
db_name = 'Banks.db'
csv_output = 'Largest_banks_data.csv'
query1= f'SELECT * FROM {table_name}'
query2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query3 = f'SELECT "Bank Name" from {table_name} LIMIT 5'

csv_path = r'C:\Users\mex_cborjaruiz\Desktop\Cesar\Data_Science\exchange_rate.csv'
# Logging process for each ETL stage.
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

def extract(url,df):
    html_data = requests.get(url).text
    data = BeautifulSoup(html_data,'html.parser')
    table_html = data.find_all('tbody')
    #print(len(table_html[0].contents))
    #print(table_html[0].contents)
    rows = table_html[0].find_all('tr')
    for row in rows:
        column = row.find_all('td')
        #print(len(column))
        if len(column) != 0:
        
            #print(len(column[1].find_all('a')))
            title = column[1].find_all('a')
            #print(title[1].contents[0])

            data_dict = {'Rank':column[0].contents[0].strip(),
                         'Bank Name':title[1].contents[0],
                         'MC_USD_Billion':column[2].contents[0].strip()}
            
            df1 = pd.DataFrame(data=data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)

    return df

def transform(csv_path,df):
    exchange_rate = pd.read_csv(csv_path)
    #print(exchange_rate.dtypes)
    df = df.astype({'MC_USD_Billion': 'float32'})
    df = df.round({'MC_USD_Billion': 2})
    new_columns = pd.DataFrame(columns=['MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion'])
    df = pd.concat([df,new_columns],axis=1)
    df['MC_GBP_Billion'] = [np.round(x*(exchange_rate.iloc[1,1]),2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*(exchange_rate.iloc[0,1]),2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*(exchange_rate.iloc[2,1]),2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df,csv_path):
    df.to_csv(csv_path)

def load_to_db(table,db,df):
    conn = sqlite3.connect(db)
    df.to_sql(table,conn,if_exists='replace',index=False)

def query_db(db,query):
    conn = sqlite3.connect(db)
    select = pd.read_sql(sql=query,con=conn)
    return select


log_progress("ETL Begin")
log_progress("Extract Process Started")
df = extract(url,df)
log_progress("Extract Process Finished")

log_progress("Transform Process Started")
transformed_data = transform(csv_path,df)
log_progress("Transform Process Finished")

log_progress("Load Process Started")
load_to_csv(df,csv_output)
load_to_db(table_name,db_name,transformed_data)
log_progress("Load Process Finished")

log_progress("Query Process Started")
query1_response = query_db(db=db_name,query=query1)
print(query1_response)

query2_response = query_db(db=db_name,query=query2)
print(query2_response)

query3_response = query_db(db=db_name,query=query3)
print(query3_response)
log_progress("Query Process Finished")
log_progress("ETL Process Finished")

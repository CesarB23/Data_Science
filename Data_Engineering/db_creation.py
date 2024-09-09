import sqlite3
import pandas as pd

# DB Parameters
db_conn = 'staff.db'
table_name  = 'staff'
# Csv Path
csv_path = r'C:\Users\mex_cborjaruiz\Desktop\Cesar\Data_Science\INSTRUCTOR.csv'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
df = pd.read_csv(csv_path,names=attribute_list)
#print(df.columns)

# DB Connection
conn = sqlite3.connect(db_conn)
df.to_sql(name=table_name,con=conn,if_exists='replace',index=False)

# Query Strings for read_sql
query = f"SELECT * FROM {table_name}"
query1 = f"SELECT FNAME FROM {table_name}"

select = pd.read_sql(sql=query1,con=conn)

# Append new data to db via pandas
data_dict = {'ID':[100],
              'FNAME':["John"], 
              'LNAME':["Doe"],
              'CITY': ["Paris"],
              'CCODE':['Fr']}

new_data = pd.DataFrame(data=data_dict)
new_data.to_sql(name=table_name,con=conn,if_exists='append',index=False)
print("New Data Appended\n")
# Qeury for appended data
query2 = f"SELECT * FROM {table_name} WHERE LNAME = 'Doe'"

# Alternative Sintaxis for querys for not harcoding values
"""
query2 = f"SELECT * FROM {table_name} WHERE FNAME = ?"

# Parámetro que será reemplazado en la consulta
params = ('John',)
"""

select = pd.read_sql(sql=query2,con=conn)
print(select)

conn.close()


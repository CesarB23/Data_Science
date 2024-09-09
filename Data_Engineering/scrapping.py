from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import requests
print(sqlite3.sqlite_version)

db_name = 'movies.db'
table_name = 'top50'
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
count = 0 
csv_path = 'top50movies.csv'

html = requests.get(url).text
df = pd.DataFrame(columns=['Average Rank','Film','Year'])
soup = BeautifulSoup(html,'html.parser')
#print(soup.find_all("tbody"))
table = soup.find_all("tbody")
# Contains each Table row, each 8 positions of the information within the table
rows = table[0].find_all("tr")
# A total of 109 rows
#print(len(rows))
# Each row contains 8 cells columns of information where is located the meaningfull information
for row in rows:
    if count < 50:
        col = row.find_all("td")
        if len(col) != 0:
            #print(col[0].contents[0])
            #print(col[1].contents[0])
            #print(col[2].contents[0])
            data_dict = {"Average Rank":col[0].contents[0],
                        "Film":col[1].contents[0],
                        "Year":col[2].contents[0]} 
            
            df_scrapped = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df_scrapped],ignore_index=True)
            count +=1

df.to_csv(csv_path,index=False)

conn = sqlite3.connect(db_name)
df.to_sql(table_name,conn,if_exists='replace',index=False)
conn.close()

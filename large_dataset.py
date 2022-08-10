import db_connection
import pandas as pd
import time 

ts = time.time()
df = pd.read_csv('datasets/animelists_cleaned.csv',nrows=3000000)
conn = db_connection.get_engine().connect()
df.to_sql('animes',conn,if_exists='replace',index_label='indice')

print(f'Tiempo transcurrido: {time.time()-ts}')
conn.close()
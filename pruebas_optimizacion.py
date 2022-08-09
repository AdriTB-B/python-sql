
import pandas as pd
import db_connection
import multiprocessing as mp
import time

df_sales = None
dfs = []

def load_df(*args):    
    sql = f'select * from sales.SalesOrderDetail order by SalesOrderDetailID OFFSET {args[0][0]} ROWS FETCH NEXT {args[0][1]} ROWS ONLY '
    print(sql)
    return pd.read_sql(
        sql,
        args[0][2],
        index_col='SalesOrderDetailID'
        )

if __name__=='__main__':
    start_time = time.time()
    #########################
    with mp.Pool(mp.cpu_count()) as pool: 
        conn = db_connection.get_engine().connect()     
        steps = [(x,15000,conn) for x in range(0,120000,15000)]
        print(steps)
        dfs = pool.map(load_df,steps)       
   
        df_sales = pd.concat(dfs)
        print(df_sales.count())
        conn.close()
   
        
    #########################    
    print(f'Demora: {time.time() - start_time}')
    





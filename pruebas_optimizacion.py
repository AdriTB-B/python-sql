
import pandas as pd
import db_connection
import multiprocessing as mp
import time

df_sales = None
dfs = []

def load_df(*args):   
    with db_connection.get_engine().connect() as conn:  
        sql = f'select * from sales.SalesOrderDetail order by SalesOrderDetailID OFFSET {args[0][0]} ROWS FETCH NEXT {args[0][1]} ROWS ONLY '
        print(sql)
        return pd.read_sql(
            sql,
            conn,
            index_col='SalesOrderDetailID'
            )
def get_count():
    with db_connection.get_engine().connect() as conn:
        count = conn.execute('select count(*) from sales.SalesOrderDetail').fetchone()[0]
        print(f'Registros a encontrar: {count}')
        return count

if __name__=='__main__':
    start_time = time.time()
    #########################
    with mp.Pool(mp.cpu_count()) as pool:
        reg_count = get_count()
        cpus = mp.cpu_count()
        limit = reg_count//8
        steps = [(x,limit) for x in range(0,reg_count,limit)]
        print(f'Particiones: {steps}')
        dfs = pool.map(load_df,steps)       
   
        df_sales = pd.concat(dfs)
        print(f'Registros obtenidos: {df_sales.count()}')
    #########################    
    print(f'Demora: {time.time() - start_time}')
    





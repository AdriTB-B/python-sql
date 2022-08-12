
import pandas as pd
import db_connection
import multiprocessing as mp
import time
import logging

table = 'dbo.animes'
id = 'indice'
conn = db_connection.connection()
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')

def load_df(*args):
    # sql = f'select * from {table} order by [{id}] OFFSET {args[0][0]} ROWS FETCH NEXT {args[0][1]} ROWS ONLY'
    sql = f'select * from {table} where {id} between {args[0]} and {args[1] + args[0]}'
    # logging.info(sql)
    df = pd.read_sql(
        sql,
        conn,
        index_col=id
        )
    # logging.info(f'*Parte obtenida*: {args[0]}')
    return df

def get_count():
    with db_connection.get_engine().connect() as conn:
        count = conn.execute(f'select count(*) from {table}').fetchone()[0]
        # print(f'Registros a encontrar: {count}')
        return count

def multi():
    ### Multiproceso
    reg_count = get_count()
    start_time = time.time()
    cpus = mp.cpu_count()
    with mp.Pool(cpus) as pool:
        limit = reg_count//cpus
        steps = [(x,limit) for x in range(0,reg_count,limit)]
        # print(f'Particiones: {steps}')
        dfs = pool.starmap(load_df,steps)
        data = pd.concat(dfs)
        print(f'Demora de multiproceso: {time.time() - start_time}')
    return data

def one():
    ### Un proceso
    start_time = time.time()
    sql = f'select * from {table}'
    data = pd.read_sql(
            sql,
            conn,
            index_col=id
            )
    print(f'Demora de uniproceso: {time.time() - start_time}')
    return data

def get_count():
    with db_connection.get_engine().connect() as conn:
        count = conn.execute(f'select count(*) from {table}').fetchone()[0]
        print(f'Registros a encontrar: {count}')
        return count

def main():
    print('Inicio de las consultas')
    #########################
    df_sales = multi()
    df_sales = one()
    conn.close()
    # print(f'Registros obtenidos: {df_sales.count()}')
    print(df_sales.head(10))
    #########################  

if __name__=='__main__':
    main()




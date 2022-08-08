import pandas as pd
import db_connection

with db_connection.get_engine().connect() as conn:
    df_sales = pd.read_sql('select * from sales.SalesOrderDetail',conn,index_col='SalesOrderID')
    print(df_sales)

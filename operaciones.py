import db_connection
import pandas as pd

with db_connection.connection() as conn:
    # Operaciones
    # Obtener empresas
    df_empresas = pd.read_sql('select * from dbo.empresas',conn)
    print(f'Empresas\n{df_empresas}')

    # Obtener regulacion europea
    df_eeuu_reg = pd.read_sql('select * from dbo.eeuu_reg',conn)
    print(f'Regulacion europea\n{df_eeuu_reg}')

    # Añadir columna a empresas
    # conn.execute("""alter table dbo.empresas add interes numeric(18,0)""")
    df_empresas['interes'] = df_empresas['prestamo'] * (df_empresas['porcentaje']/100)
    print(df_empresas)
    df_empresas.to_sql(name='dbo.empresas',con=conn, if_exists='replace')



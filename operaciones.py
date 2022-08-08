import db_connection
import pandas as pd


with db_connection.connection() as conn:
    # Operaciones
    # Obtener empresas
    df_empresas = pd.read_sql('select * from dbo.proy_empresa',conn,index_col='id')
    print(f'Empresas\n{df_empresas}')

    # Obtener regulacion europea
    df_ue_reg = pd.read_sql('select * from dbo.proy_reg_ue',conn, index_col='id')
    print(f'Regulacion europea\n{df_ue_reg}')

    # Añadir columna a empresas
    # conn.execute("""alter table dbo.proy_empresa add interes as prestamo * porcentaje""")
    print(df_empresas)

with db_connection.get_engine().connect() as conn:
    # Crear tabla común con SQLAlchemy
    df_proy_ue = df_empresas[df_empresas['nombre_proyecto'].isin(df_ue_reg['nombre_proyecto'])]
    print(f'Tabla común de proyectos\n{df_proy_ue}')
    df_proy_ue.to_sql('proy_ue',conn,if_exists='replace')



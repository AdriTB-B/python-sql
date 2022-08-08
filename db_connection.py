import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configuración
driver='{ODBC Driver 17 for SQL server}'
server='L2205014'
db='empresas'
uid='BOSONIT\adrian.tena'
pwd=''
cnx_str = (f"""Driver={driver};
                Server={server};
                Database={db};
                Trusted_Connection=yes;
                UID={uid};
                PWD={pwd}""")
cnx_url = URL.create("mssql+pyodbc", query={"odbc_connect": cnx_str})


def connection():
    try:
        return pyodbc.connect(cnx_str)
    except pyodbc.Error as error:
        print(error)

def get_engine():
    return create_engine(cnx_url)


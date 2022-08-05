import pyodbc


def connection():
    driver='{ODBC Driver 17 for SQL server}'
    server='L2205014'
    db='empresas'
    uid='BOSONIT\adrian.tena'
    cnx_str = (f"""Driver={driver};
                Server={server};
                Database={db};
                Trusted_Connection=yes;
                UID={uid};""")
    try:
        return pyodbc.connect(cnx_str)
    except pyodbc.Error as error:
        print(error)

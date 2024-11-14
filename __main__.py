from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import pandas as pd

from dotenv import load_dotenv
import os

def connect_to_snowflake():
    load_dotenv()  # carga las variables de entorno del archivo .env

    USER = os.getenv('SNOWFLAKE_USER')
    PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT_IDENTIFIER')

    print(f"USER: {USER}")
    print(f"PASSWORD: {PASSWORD}")
    print(f"ACCOUNT: {ACCOUNT}")

    # Definición del motor de conexión a Snowflake
    engine = create_engine(URL(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse='COMPUTE_WH',
        database='PRIMERABBDD',
        schema='PRIMERESQUEMA',
        role='ACCOUNTADMIN'
    ))
    return engine

def read_from_snowflake(engine):
    # Leer de una tabla de Snowflake
    df = pd.read_sql_query("SELECT * FROM employees", engine)
    print(df)
    return df

def write_to_snowflake(engine):
    # Escribir en una tabla Snowflake
    from snowflake.connector.pandas_tools import pd_writer
    # ¿Qué hacer si la tabla ya existe? replace, append, o fail?
    if_exists = 'replace'
    data = {
        'ORDER_ID': ['O001', 'O002', 'O003'],
        'AMOUNT': [10000, 15000, 20000],
        'PROFIT': [200, 300, 400],
        'QUANTITY': [10, 15, 20],
        'CATEGORY': ['Electronics', 'Furniture', 'Office Supplies'],
        'SUBCATEGORY': ['Phones', 'Chairs', 'Paper']
    }
    df = pd.DataFrame(data)
    with engine.connect() as con:
        df.to_sql(name='orders'.lower(), con=con, if_exists=if_exists, method=pd_writer, index=False)

def main():
    engine = connect_to_snowflake()
    read_from_snowflake(engine)
    write_to_snowflake(engine)

if __name__ == "__main__":
    main()
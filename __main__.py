from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import pandas as pd

from dotenv import load_dotenv
import os

def connect_to_snowflake(warehouse, database, schema, role):
    load_dotenv()  # carga las variables de entorno del archivo .env

    USER = os.getenv('SNOWFLAKE_USER')
    PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT_IDENTIFIER')

    # Definición del motor de conexión a Snowflake
    engine = create_engine(URL(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    ))
    return engine

def read_from_snowflake(engine):
    # Leer de una tabla de Snowflake
    df = pd.read_sql_query("SELECT * FROM employees", engine)
    print(df.head(5))
    return df

def write_to_snowflake(engine, table_name):
    # Escribir en una tabla Snowflake
    from snowflake.connector.pandas_tools import pd_writer
    # ¿Qué hacer si la tabla ya existe? replace, append, o fail?
    if_exists = 'replace'
    data = {
        'ORDER_ID': ['O001', 'O002', 'O003'],
        'AMOUNT': [11111, 22222, 33333],
        'PROFIT': [200, 300, 400],
        'QUANTITY': [10, 15, 20],
        'CATEGORY': ['Electronics', 'Furniture', 'Office Supplies'],
        'SUBCATEGORY': ['Phones', 'Chairs', 'Paper']
    }
    df = pd.DataFrame(data)
    with engine.connect() as con:
        df.to_sql(name=table_name, con=con, if_exists=if_exists, method=pd_writer, index=False)

def main():
    engine = connect_to_snowflake(warehouse='COMPUTE_WH', database='PRIMERABBDD', schema='PRIMERESQUEMA', role='ACCOUNTADMIN')
    read_from_snowflake(engine)
    write_to_snowflake(engine, table_name='orders')

if __name__ == "__main__":
    main()
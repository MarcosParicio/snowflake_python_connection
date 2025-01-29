from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
# Escribir en una tabla Snowflake
from snowflake.connector.pandas_tools import pd_writer

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
    # ¿Qué hacer si la tabla ya existe: replace, append, o fail?
    if_exists = 'append'
    data = {
        'ID': [204, 205, 206],
        'FIRST_NAME': ['Alice', 'Bob', 'Charlie'],
        'LAST_NAME': ['Johnson', 'Smith', 'Brown'],
        'EMAIL': ['alice.johnson@example.com', 'bob.smith@example.com', 'charlie.brown@example.com'],
        'LOCATION': ['Madrid', 'Barcelona', 'Valencia'],
        'DEPARTMENT': ['Human Resources', 'Marketing', 'Services']
    }
    df = pd.DataFrame(data)
    with engine.connect() as con:
        df.to_sql(name=table_name, con=con, if_exists=if_exists, method=pd_writer, index=False)

def main():
    engine = connect_to_snowflake(warehouse='COMPUTE_WH', database='PRIMERABBDD', schema='PRIMERESQUEMA', role='ACCOUNTADMIN')
    read_from_snowflake(engine)
    write_to_snowflake(engine, table_name='employees')

if __name__ == "__main__":
    main()
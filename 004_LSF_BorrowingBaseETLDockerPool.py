

from prefect import flow, task
import time
from pathlib import Path
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, VARCHAR, DATETIME, BIGINT, FLOAT, ForeignKey
from sqlalchemy import inspect
import pyodbc
import os
import platform
import socket
import sqlite3


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


@task
def extract_data_lsf (dataset_url: str) -> pd.DataFrame:
    #importing the file
    df = pd.read_excel (dataset_url, parse_dates = True)
    return (df)

@task
def clean_data_lsf (df) -> pd.DataFrame:
    #changing datatypes
    df ['dollaramount'] = df ['dollaramount'].astype ('float')
    #making all columns lower case
    df.columns = df.columns.str.lower()
    return (df)

@task
def load_data_lsf (df):
    #changing the working directory to where I want it
    os.chdir('C:\\Users\\dustinh\\Desktop\\Pandas')

    #creating a table in SQL using python
    metadata = MetaData ()
    b_lsfborrowingbase = Table ('b_lsfborrowingbase', metadata,
                        Column ('dollaramount', FLOAT),
                               )
    
    #making the SQL connection to sqlalchemy and assigning it to a 'conn' variable. This is going to the import Datawarehouse database
    database_address ='mssql+pymysql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine('mssql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server', echo = False)
    metadata.create_all(engine)
    
    conn = sqlalchemy.create_engine('mssql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')

    conn.connect()

    df.to_sql('b_lsfborrowingbase', con = conn, if_exists = "replace", index=False,
              dtype = {'dollaramount': sqlalchemy.types.Float(precision=3, asdecimal=True), 
                      }           
             )
    print ('Success')

@flow
def etl_lsf():
    dataset_url = r'C:\data-analysis\_shared\b_excel_files\3d_lsfborrowingbase\LSFShiven.xlsx'
    df = extract_data_lsf (dataset_url) 
    df = clean_data_lsf (df)
    df_load = load_data_lsf (df)
    
if __name__ == "__main__":
                etl_lsf.deploy(
                      name = 'lsf_etl_deployment_docker',
                      work_pool_name= 'my-docker-pool',
                      image = 'my-first-prefect-docker-imgae')


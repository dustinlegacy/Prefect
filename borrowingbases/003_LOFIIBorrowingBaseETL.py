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
def extract_data_lofII (dataset_url: str) -> pd.DataFrame:
    #importing the file
    df = pd.read_excel (dataset_url, parse_dates = True)
    return (df)


@task
def transform_data_lofII (df) -> pd.DataFrame:
    df = df.rename(columns = {'Unnamed: 1':'1', 'Unnamed: 2':'2','Unnamed: 3': '3', 'Unnamed: 4':'4', 'Unnamed: 5':'5', \
                                 'Unnamed: 6':'6', 'Unnamed: 7': '7', 'Unnamed: 8': '8', 'Unnamed: 9': '9', 'Unnamed: 10': '10', 'Unnamed: 11':'11', \
                                'Unnamed: 12' :'12', 'Unnamed: 13': '13', 'Unnamed: 14' : '14', 'Unnamed: 15':'15', 'Unnamed: 16' : '16',\
                                'Unnamed: 17': 'title', 'Unnamed: 18' : '18', 'Unnamed: 19': '19', 'Unnamed: 20': '20', 'Unnamed: 21': '21',\
                                'Unnamed: 22': 'values', 'Unnamed: 23': '23', 'Unnamed: 24': '24', 'Unnamed: 25' : '25', 'Unnamed: 26':'26', \
                                'Unnamed: 27': '27', 'Unnamed: 28': '28', 'Unnamed: 29': '29', 'Unnamed: 30':'30', 'Unnamed: 31': '31',\
                                'Unnamed: 32' : '32','Unnamed: 33': '33'})
    
    condition = (df['20'] == "Line of Credit and Available Borrowing Base as of:") | (df['20'] == "Total Borrowing Base") | (df['20'] == "Line of Credit and Available Borrowing Base as")
    
    first_occurrence = df[condition].index[0]
    
    df = df.iloc[first_occurrence:]

    columns_to_keep = ["20", "24"]
    
    df = df [columns_to_keep]

    df = df.dropna(axis = 0)

    df.columns = [''] * len(df.columns)

    df = df.transpose ()

    df.columns = df.iloc[0]

    df = df.iloc [1:,:]

    #creating a new column that adds a Fund 
    Fund = ['LOFII']

    #adding that new column to the dataframe
    df ['Fund'] = Fund

    df.columns.values[0:8] =["Date", "SecuredCommitment", "AvailableBorrowingBaseNonConstruction", \
                             "AvailableBorrowingBaseConstruction", "TotalBorrowingBase", "MaxLineofCreditAvailability", "OutstandingLineofCreditBalance",\
                            "AvailableBorrowingBase"]
    
    df.columns = df.columns.str.strip()

    #changing datatypes of columns
    df ['Date'] = pd.to_datetime (df ['Date'])
    df ['SecuredCommitment'] = df ['SecuredCommitment'].astype ('float')
    df ['AvailableBorrowingBaseNonConstruction'] = df ['AvailableBorrowingBaseNonConstruction'].astype ('float')
    df ['AvailableBorrowingBaseConstruction'] = df ['AvailableBorrowingBaseConstruction'].astype ('float')
    df ['TotalBorrowingBase'] = df ['TotalBorrowingBase'].astype ('float')
    df ['MaxLineofCreditAvailability'] = df ['MaxLineofCreditAvailability'].astype ('float')
    df ['OutstandingLineofCreditBalance'] = df ['OutstandingLineofCreditBalance'].astype ('float')
    df ['AvailableBorrowingBase'] = df ['AvailableBorrowingBase'].astype ('float')
    df ['Fund'] = df ['Fund'].astype ('string')

    #making all columns lower case
    df.columns = df.columns.str.lower()

    return (df)


@task
def load_data_lofII (df):
    #changing the working directory to where I want it
    os.chdir('C:\\Users\\dustinh\\Desktop\\Pandas')

     #creating a table in SQL using python
    metadata = MetaData ()
    b_lofIIborrowingbase = Table ('b_lofIIborrowingbase', metadata,
                    Column ('date', DATETIME),
                    Column ('securedcommitment', FLOAT),
                    Column ('availableborrowingbasenonconstruction', FLOAT),
                    Column ('availableborrowingbaseconstruction', FLOAT),
                    Column ('totalborrowingbase', FLOAT),
                    Column ('maxlineofcreditavailability', FLOAT),
                    Column ('outstandinglineofcreditbalance', FLOAT),
                    Column ('availableborrowingbase', FLOAT),
                    Column ('fund', VARCHAR (20)),
                           )
    
    
    #making the SQL connection to sqlalchemy and assigning it to a 'conn' variable. This is going to the import Datawarehouse database
    database_address ='mssql+pymysql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
    engine = sqlalchemy.create_engine('mssql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server', echo = False)
    metadata.create_all(engine)

    conn = sqlalchemy.create_engine('mssql://LGPC24\SQLEXPRESS/DataStagingArea?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')

    conn.connect()

    df.to_sql('b_lofIIborrowingbase', con = conn, if_exists = "replace", index=False,
              dtype = {'date': sqlalchemy.types.DATETIME(),
                       'securedcommitment': sqlalchemy.types.Float(precision=3, asdecimal=True), 
                       'availableborrowingbasenonconstruction': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'availableborrowingbaseconstruction': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'totalborrowingbase': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'maxlineofcreditavailability': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'outstandinglineofcreditbalance': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'availableborrowingbase': sqlalchemy.types.Float(precision=3, asdecimal=True),
                       'fund': sqlalchemy.types.VARCHAR()}           
                       )
    
@flow
def etl_lofII():
    dataset_url = r'C:\data-analysis\_shared\b_excel_files\3c_lofIIborrowingbase\LOFIIBorrowingBase(Current).xlsx'
    df = extract_data_lofII (dataset_url)
    df_transformed = transform_data_lofII (df)
    df_load = load_data_lofII (df_transformed)

    
if __name__ == "__main__":
                etl_lofII()







    











    













    

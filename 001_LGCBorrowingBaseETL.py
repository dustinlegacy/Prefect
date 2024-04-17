from prefect import flow, task
import time
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
from prefect_github.repository import GitHubRepository

github_repository_block = GitHubRepository.load("githubblocki")



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)




@task
def extract_data_lgc(dataset_url: str) -> pd.DataFrame:
    #importing the file
    df = pd.read_excel (dataset_url, parse_dates = True)
    return (df)

@task
def transorm_data_lgc (df):
    #changing the the names of the all columns 
    df = df.rename(columns = {'Unnamed: 1':'crnumber', 'Unnamed: 2':'loannumber', 'Unnamed: 4':'builder', 'Unnamed: 5':'propertyaddress',                                                         'Unnamed: 6':'code', 'Unnamed: 7': 'units', 'Unnamed: 8': 'city', 'Unnamed: 9': 'state', 'Unnamed: 10': 'county', 'Unnamed: 11':'zip',                                                         'Unnamed: 12' :'sfr', 'Unnamed: 13': 'multiunit', 'Unnamed: 14' : 'notedate', 'Unnamed: 15':'noteexpiry', 'Unnamed: 16' : 'pledgedate',                                                        'Unnamed: 17': 'removedate', 'Unnamed: 18' : 'availableborrowingbase', 'Unnamed: 19': 'noteamount', 'Unnamed: 20': 'purchaseprice', 'Unnamed: 21': 'propertyvalue',
                                                        'Unnamed: 22': 'advancelimit', 'Unnamed: 23': 'ltvlimt', 'Unnamed: 24': 'programlimit', 'Unnamed: 25' : 'conmax5000000', 'Unnamed: 26':'ineligiblecollateral', \
                                                        'Unnamed: 27': 'maximumborrowingbase', 'Unnamed: 28': 'advanceconc', 'Unnamed: 30':'12molimit', 'Unnamed: 31': 'removalreason', 'Unnamed: 32' : 'daysonline',\
                                                        'Unnamed: 33': 'comments'})
    # Assuming df is your DataFrame
    condition = (df['advancelimit'] == "Line of Credit and Available Borrowing Base as of:") | (df['advancelimit'] == "Total Borrowing Base") | (df['advancelimit'] == "Line of Credit and Available Borrowing Base as")
    first_occurrence = df[condition].index[0]
    df = df.iloc[first_occurrence:]
    
    columns_to_keep = ["advancelimit", "ineligiblecollateral"]
    df = df[columns_to_keep]
    
    df = df.dropna(axis = 0)
    
    df.columns = [''] * len(df.columns)
    
    df = df.transpose ()
    
    #refrencing the first row
    df.columns = df.iloc[0]
    #using that new index as the new column headers
    df = df.iloc [1:,:]
    
    #creating a new column to noteate the fund
    Fund = ['NW']
    #adding the new column to the dataframe
    df ['Fund'] = Fund
    
    df.columns.values[0:8] =["Date", "SecuredCommitment", "AvailableBorrowingBaseNonConstruction",                                           "AvailableBorrowingBaseConstruction", "TotalBorrowingBase", "MaxLineofCreditAvailability", "OutstandingLineofCreditBalance",                                          "AvailableBorrowingBase"]
    
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
    df.columns  = df.columns.str.replace('\s+','')
    return (df)

@task
def load_data_lgc (df):
    #chaning the working directory to where I want it
    os.chdir('C:\\Users\\dustinh\\Desktop\\Pandas')
    #creating a table in SQL using python
    metadata = MetaData ()
    b_lgcborrowingbase = Table ('b_lgcborrowingbase', metadata,
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
    
    df.to_sql('b_lgcborrowingbase', con = conn, if_exists = "replace", index=False,
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
    
    print ('Success')
    
    
    
    
    




@flow
def etl_lgc():
    dataset_url = r'C:\data-analysis\_shared\b_excel_files\3a_lgcborrowingbase\LGCBorrowingBase(Current).xlsx'
    df = extract_data_lgc (dataset_url)
    df_transformed = transorm_data_lgc (df)
    df_load = load_data_lgc (df_transformed)

    
if __name__ == "__main__":
                etl_lgc()

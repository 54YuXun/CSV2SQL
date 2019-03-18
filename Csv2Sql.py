import ssl
import sys
import json
import argparse
import pandas as pd
import sqlalchemy as sa

def get_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", dest="f", action="store", help="key in your csv filename", nargs=1)
    parser.add_argument("-t", dest="t", action="store", help="key in your tablename", nargs=1)
    f = parser.parse_args().f
    t = parser.parse_args().t
    csvfile = f[0]
    tablename = t[0]  
    return csvfile, tablename

def sql_config():
  try:
    with open('Config.json' , 'r') as jsondata:
        itemSet = json.loads(jsondata.read())
        ip_address = itemSet[0]['address']
        dbname = itemSet[0]['dbname']
        account = itemSet[0]['account']
        password = itemSet[0]['password']
        engine = sa.create_engine('mssql+pyodbc://'+account+':'+password+'@'+ip_address+':1433/'+dbname+'?driver=SQL+Server+Native+Client+11.0')
        return engine
  except:
    print('File is not existed.')
    sys.exit(0)

def readcsv(csvfile):
    try:
        df = pd.read_csv(csvfile,encoding ='utf-8')
    except: 
        df = pd.read_csv(csvfile,encoding ='big5')
    return df

def csv2sql(tablename,dataframe, engine):
    try:
      dataframe.to_sql(tablename, engine, if_exists='replace',index_label = 'Index')
      print('Create Table Succeeded ')
    except:
      print('Create Table Failed')
      sys.exit(0)

def main():
    ssl._create_default_https_context = ssl._create_unverified_context
    engine = sql_config()
    csvfile, tablename = get_argument()
    dataframe = readcsv(csvfile)
    csv2sql(tablename, dataframe, engine)

if __name__ == "__main__":
    main()


import requests
import pandas as pd
from sqlite3 import connect
from sqlalchemy import create_engine
from sean_keys import chuck_key

if __name__ == '__main__':
  engine = create_engine('sqlite:///sentiment.db', echo = True)
  sqlite_connection = engine.connect()
  sqlite_table = "tsla_stock_prices"

  params = {
    'access_key': chuck_key
  }

  api_result = requests.get('https://api.marketstack.com/v1/intraday?api_key=f{}&symbols=TSLA&interval=30min'.format(params['access_key']),params)

  api_response = api_result.json()

  #for dic in api_response['data']:
  #   print(dic['date'], dic['last'])



  df = pd.DataFrame(api_response['data'])
  #print(api_response)
  #print(df)
  #df.to_csv('stock.csv')
  df.to_sql(sqlite_table, sqlite_connection, if_exists = 'replace')
  
  sqlite_connection.close()
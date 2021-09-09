import pandas as pd
import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine
from datetime import date, datetime
from pytz import timezone
import numpy as np




def main5(date):
    # connect to db
    con = sqlite3.connect('sentiment.db')
    df = pd.read_sql_query(f"SELECT * FROM tsla_stock_prices WHERE date LIKE '{date}%' ", con)
    
    # create new columns, last and date
    df_2_columns = df[['last', 'date']]
    # clean up 'T' and '+' from strings
    for index, row in df_2_columns['date'].iteritems():
        df_2_columns.loc[index, 'date'] = row.replace('T', ' ').replace('+', '')
    
    eastern = timezone('US/Eastern')
    utc = timezone('UTC')

    # "%d/%m/%Y %H:%M:%S"
    # format as eastern
    for index, row in df_2_columns['date'].iteritems():
        
        created_at = datetime.strptime(row[:-5], '%Y-%m-%d %H:%M:%S')
        utc_created_at = utc.localize(created_at)
        #print(utc_created_at)
        row = utc_created_at.astimezone(eastern)
        df_2_columns.loc[index, 'date_est'] = str(row)  
    
    df_2_columns.drop(columns = 'date')

    # slice for date and time
    for index, row in df_2_columns['date_est'].iteritems():
    #print(row[:10])
    # time is currently utc
        df_2_columns.loc[index, 'time'] = row[11:16]
        df_2_columns.loc[index, 'new_date'] = row[:10]
    
    df_2_columns = df_2_columns.drop(columns = 'date')

    df_2_columns = df_2_columns.drop(columns = 'date_est')

    df_3 = df_2_columns
    #print('df3/n/n/n/n/n', df_3)
    # our select statement, probably gonna modify
    df_4 = df_3[df_3['new_date'].str.match(f'{date}')]


    df_4 = df_4.sort_values(by = ['time'])
    #print('df_4\n\n\n\n\n', df_4)
    df_5 = pd.DataFrame()

    # create a new df without values after 4:30
    for index, row in df_4.iterrows():
        print(row)
        print('hello')
        if row['time'] != '16:30':
            df_5 = df_5.append(row)
        else:
            break
    df_5 = df_5.drop_duplicates().reset_index(drop = True)
    df_5 = df_5.fillna(df_5.mean())

    engine = create_engine('sqlite:///sentiment.db', echo = True)
    sqlite_connection = engine.connect()

    sqlite_table = "tsla_prices_cleaned"
    

    # leave this as replace
    df_5.to_sql(sqlite_table, sqlite_connection, if_exists = 'replace')
    sqlite_connection.close()
    print(df_5)

if __name__ == '__main__':
    from datetime import date
    from datetime import timedelta
    today = date.today()
    yesterday = today - timedelta(days = 1)
    main5(yesterday)

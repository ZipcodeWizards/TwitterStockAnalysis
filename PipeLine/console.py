import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine
import pandas as pd
import cleaning_spark
import cleaning_nlp
import cleaning_tsla_prices
import plotly_graphs
import main


def dates_from_twitter():
    con = sqlite3.connect('sentiment.db')
    df = pd.read_sql_query("SELECT DISTINCT date_time FROM sentiment", con)
    df['date'] = df.date_time.str[:10]
    twdates = df.date.unique()
    con.close()
    return twdates

def dates_from_stock():
    con = sqlite3.connect('sentiment.db')
    df = pd.read_sql_query("SELECT DISTINCT date FROM tsla_stock_prices", con)
    df['date1'] = df.date.str[:10]
    twdates = df.date1.unique()
    con.close()
    return twdates

if __name__ == "__main__":
    choice = input('Would you like run todays data(1) or pick a old data(2) :')
    if choice == '1':
        main.run_it_all()
    else:
        l1 = dates_from_twitter()
        l2 = dates_from_stock()
        l3 = []
        for d in l1:
            for s in l2:
                if d == s:
                    l3.append(d)
        if len(l3) == 0:
            print('no old data to run')
        else:
            for i in l3:
                print(i)
            date = str(input("Pick a date to analyze :"))
            cleaning_spark.main2(date)
            cleaning_nlp.main3(date)
            cleaning_tsla_prices.main5(date)
            plotly_graphs.main6(date)
    

     



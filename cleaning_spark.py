from pyspark.sql import SparkSession
from pyspark.sql import column
import pandas as pd
import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine



def main2():
    con = sqlite3.connect('sentiment.db')

    df = pd.read_sql_query("SELECT * FROM sentiment WHERE date_time LIKE '2021-09-05%' ", con)

    # create SparkSession
    spark = SparkSession.builder \
            .master("local[1]") \
            .appName("Tesla Tweets") \
            .getOrCreate()

    sparkDF = spark.createDataFrame(df)
    sparkDF = sparkDF.distinct()

    # clean for retweets and only english
    sparkDFnoRT = sparkDF.filter(~sparkDF.text.startswith('RT'))
    spark_DF_En = sparkDFnoRT.filter(sparkDFnoRT.lang.contains('en'))



    # drop original index
    spark_DF_No_index = spark_DF_En.drop('index')

    spark_DF_No_index = spark_DF_No_index.drop('retweeted')

    # convert our final spark cleaned index to a pandas df 
    pandasDF = spark_DF_No_index.toPandas()

    # connect to sqlite3 database
    engine = create_engine('sqlite:///sentiment.db', echo = True)
    sqlite_connection = engine.connect()
    sqlite_table = "spark_cleaned"
    pandasDF.to_sql(sqlite_table, sqlite_connection, if_exists = 'replace')
    sqlite_connection.close()

    spark.stop()

if __name__ == '__main__':
        main2()
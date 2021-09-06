import pandas as pd
import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine
from nltk.corpus import stopwords
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import date, datetime
from pytz import timezone


if __name__ == '__main__':

    con = sqlite3.connect('/Users/sean/labs/Capstone/TwitterStockAnalysis/sentiment.db')
    
    df = pd.read_sql_query("SELECT * FROM spark_cleaned", con)
    df = df.rename(columns = {'date_time': 'date_time_est'})

    eastern = timezone('US/Eastern')
    utc = timezone('UTC')

# "%d/%m/%Y %H:%M:%S"
    for index, row in df['date_time_est'].iteritems():
        
        created_at = datetime.strptime(row[:-7], '%Y-%m-%d %H:%M:%S')
        utc_created_at = utc.localize(created_at)
        #print(utc_created_at)
        row = utc_created_at.astimezone(eastern)
        df.loc[index, 'date_time_est'] = row
        #print(est_created_at)
        #print(type(row))   

    df_drop_index = df.drop(columns = 'index')
    print('length of incoming df: ', len(df_drop_index))

    df_no_dupes = df_drop_index.drop_duplicates()
    print('length of no duplicates: ', len(df_no_dupes))

    # use regex to remove @usernames
    df_no_dupes['text'] = df_no_dupes['text'].str.replace('@[^\s]+','')

    # use regex to remove punctuation and emojis 
    df_no_dupes["text"] = df_no_dupes["text"].str.replace('[^\w\s]','')

    # make all text lowercase
    df_no_dupes['text'] = df_no_dupes['text'].apply(lambda x: x.lower())

    # remove stop words 
    stop = stopwords.words('english')
    df_no_dupes['text'] = df_no_dupes['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

    # clean up indexes
    df = df_no_dupes
    df = df.reset_index(drop=True)

    # nlp 

    df[['polarity', 'subjectivity']] = df['text'].apply(lambda Text: pd.Series(TextBlob(Text).sentiment))

    for index, row in df['text'].iteritems():
        score = SentimentIntensityAnalyzer().polarity_scores(row)
        #print(type(score))
        #print(score)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        comp = score['compound']
        if neg > pos:
            # loc looks at curent row and if neg > pos, sentiment is negative
            df.loc[index, 'sentiment'] = 'negative'
        elif pos > neg:
            df.loc[index, 'sentiment'] = 'positive'
        else:
            df.loc[index, 'sentiment'] = 'neutral'
            
        df.loc[index, 'neg'] = neg
        df.loc[index, 'neu'] = neu
        df.loc[index, 'pos'] = pos
        df.loc[index, 'compound'] = comp
    
    
    engine = create_engine('sqlite:///sentiment.db', echo = True)
    sqlite_connection = engine.connect()
    sqlite_table = "nlp_analysis"
    # change from append to replace, depending on if you want to remake a db or add to db 
    df.to_sql(sqlite_table, sqlite_connection, if_exists = 'append')
    sqlite_connection.close()
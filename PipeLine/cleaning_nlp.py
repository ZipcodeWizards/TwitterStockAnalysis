import pandas as pd
import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine
from nltk.corpus import stopwords
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import date, datetime
from pytz import timezone



def main3(date):
    con = sqlite3.connect('sentiment.db')
    

    #df = pd.read_sql_query("SELECT * FROM spark_cleaned", con)
    df = pd.read_sql_query(f"SELECT * FROM spark_cleaned WHERE date_time LIKE '{date}%' ", con)
    df = df.rename(columns = {'date_time': 'date_time_est'})    
    df = df.drop_duplicates()

    eastern = timezone('US/Eastern')
    utc = timezone('UTC')

# "%d/%m/%Y %H:%M:%S"
    for index, row in df['date_time_est'].iteritems():
        
        created_at = datetime.strptime(row[:-7], '%Y-%m-%d %H:%M:%S')
        utc_created_at = utc.localize(created_at)
        #print(utc_created_at)
        row = utc_created_at.astimezone(eastern)
        df.loc[index, 'date_time_est'] = str(row)
        #print(est_created_at)
        #print(type(row))
    
    df_drop_index = df.drop(columns = 'index')

    # df_drop_index.head()

    # remove duplicates 
    df_no_dupes = df_drop_index
    

    # use regex to remove @usernames

    df_no_dupes['text'] = df_no_dupes['text'].str.replace('@[^\s]+','')
    #re.sub('@[^\s]+','',Tweet)

    # use regex to remove punctuation and emojis 
    df_no_dupes["text"] = df_no_dupes["text"].str.replace('[^\w\s]','')

    for index, row in df_no_dupes['date_time_est'].iteritems():
    #print(row[:10])
    # time is currently utc
        df_no_dupes.loc[index, 'time'] = row[11:19]
        df_no_dupes.loc[index, 'new_date'] = row[:10]
        df_no_dupes.head()

    df_no_dupes = df_no_dupes.drop(columns = 'date_time_est')

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

    # NLP for sentiment, neg, neu, pos, compound

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

    df.to_sql(sqlite_table, sqlite_connection, if_exists = 'replace')

    sqlite_connection.close()

if __name__ == '__main__':
    from datetime import date
    from datetime import timedelta
    today = date.today()
    yesterday = today - timedelta(days = 1)
    main3(yesterday)
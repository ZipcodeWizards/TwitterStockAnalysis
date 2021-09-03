import tweepy

from sqlite3 import connect
from sean_keys import consumer_key, consumer_secret, access_token, access_token_secret
from sys import exit
import pandas as pd
from sqlalchemy import create_engine

# connect to api
def oauth_authenticate():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    return tweepy.API(auth)


def create_df(num_tweets):

    api = oauth_authenticate()
    screen_name = []
    date_time = []
    text = []
    retweeted = []
    cursor = tweepy.Cursor(api.search, q="$gme", tweet_mode = "extended").items(num_tweets)
    '''for i in cursor:
        print(i.full_text)'''

    for i in cursor:
        screen_name.append(i.user.screen_name)
        date_time.append(i.created_at)
        text.append(i.full_text)
        retweeted.append(i.retweeted)
    df = pd.DataFrame({'screen_name': screen_name, 'date_time': date_time, 'text': text, 'retweeted': retweeted})
    return df



if __name__ == "__main__":
    # modify this to change the number of requests. Never leave it empty!!
    num_tweets = 1

    df = create_df(num_tweets)
    print(df)

    # modify this to your local path 
    engine = create_engine('sqlite:////Users/sean/labs/Capstone/gme.db, echo = True, pool_pre_ping = True')
    sqlite_connection = engine.connect()
    sqlite_table = "gme"


    df.to_sql(sqlite_table, sqlite_connection, if_exists = 'append')
    sqlite_connection.close()


    
  


import tweepy
from time import sleep
from sqlite3 import connect
from sean_keys import consumer_key, consumer_secret, access_token, access_token_secret
from sys import exit
import pandas as pd
from sqlalchemy import create_engine
from os.path import isfile
from os import access, R_OK

# connect to api
def oauth_authenticate():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    return tweepy.API(auth)


def db_create(sqlite_file):
    conn = connect(sqlite_file)
    create_table =  'CREATE TABLE sentiment ('
    create_table += 'index INTEGER PRIMARY KEY AUTOINCREMENT, '
    create_table += 'screen_name VARCHAR(30), '
    create_table += 'created_at DATETIME, '
    create_table += 'tweet VARCHAR(255)'
    create_table += 'retweeted INTEGER, '
    create_table += 'lang VARCHAR(10), '

    create_table += ')'

    conn.cursor().execute(create_table)
    conn.commit()
    conn.close()

def db_file_exists(path):
  return isfile(path) and access(path, R_OK)

def create_df():

    api = oauth_authenticate()
    screen_name = []
    date_time = []
    text = []
    retweeted = []
    lang = []
    try:
        tweets = api.search(q="tesla -filter:retweets",
                         tweet_mode = "extended", 
                         lang = 'en', 
                         include_rts = False, 
                         count = 200)
    except tweepy.error.RateLimitError:
            print('timed out, too many request')
            
    all_tweets = []
    all_tweets.extend(tweets)
    oldest_id = tweets[-1].id

    # modify query to only get user.place.country_code = 'US'
    # in one of the cleaning stages, segment by state in user.place.full_name which is a str. I.e. Boulder, CO

    #change for to while true for max data
    #for _ in range():
    while True:
        try:
            tweets = api.search(q="tesla -filter:retweets", 
                            tweet_mode = "extended", 
                            lang = 'en', 
                            include_rts = False, 
                            count = 200, 
                            max_id = oldest_id -1)
        except tweepy.error.RateLimitError:
            print('timed out, too many request')
            break              
        oldest_id = tweets[-1].id
        all_tweets.extend(tweets)
        print('N of tweets downloaded till now {}'.format(len(all_tweets)))

    for tweet in all_tweets:
        screen_name.append(tweet.user.screen_name)
        date_time.append(tweet.created_at)
        text.append(tweet.full_text)
        retweeted.append(tweet.retweeted)
        lang.append(tweet.lang)
    df = pd.DataFrame({'screen_name': screen_name, 'date_time': date_time, 'text': text, 'retweeted': retweeted, 'lang': lang})
    
    return df




if __name__ == "__main__":
   
    df = create_df()
    print(df)

    if not db_file_exists('sentiment.db'):
        db_create('sentiment.db')

    # modify this to your local path 
    engine = create_engine('sqlite:///sentiment.db', echo = True)
    sqlite_connection = engine.connect()
    sqlite_table = "sentiment"

    # create table schema


    df.to_sql(sqlite_table, sqlite_connection, if_exists = 'append')
    sqlite_connection.close()


    
  


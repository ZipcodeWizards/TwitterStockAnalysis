# TwitterStockAnalysis
lets git it done

## Goals

- We want to compare Stock Performance vs Twitter Sentiment of a company. We will be plotting a stock's price vs average sentiment of a company on Twitter for every 30 minuteswhile the stock market is open.

## Steps to achieve goals
- twitter api to stream data into a dataframe, can get 18000 per 15 minutes.
- output df into a sqlite3 database (lake).
- clean database with spark: get rid of retweets and only use english tweets, store in new table (spark_cleaned).
- output spark_cleaned table from sqlite3 to a pandas dataframe for nltk 
- perform NLP sentiment analysis - Textblob and SentimentIntensityAnalyzer 
- set up historic stock api, MarketStack (basic teir)
- get price every half hour and group tweets by half hour and avg the sentiment scores.
- perform data visualizations with plotly and wordcloud

## Stretch goals
- flask to make it interactive 
- place code into docker and then have kubernetes run the code
- create a dash webapp with plotly
- upload to heroku

#### api > pandas df > sql database > pull into pyspark df (set time frames)> clean pyspark df > load back into a new db > put (time framed) data back into a pandas df > create plotly visualizations from timeframed pandas df > python creates vizs that are then placed into a slide deck

## Running the program
- load up Virtual Environment and load imports
    - $source leaf/bin/activate
    - $pip install -r requirements.txt

- create 'sean_keys.py' file in Pipeline folder
    - inside add the keys for api's
    - consumer_key = 'twitter key'
    - consumer_secret = 'twitter key'
    - access_token = 'twitter key'
    - access_token_secret = 'twitter key'
    - chuck_key = 'marketstack key'

- run main.py inside PipeLine folder (cd into folder)
    - for best results edit main.py based on time of day (before 9 am use 'yesterday', after 6:30 pm use 'today')
    - plot will open in new broswer tab, word cloud will be a png image in PipeLine (wc.png)

### Setting Up your SQLite3 Database

- create a sentiment.db in your local directory (inside PipeLine)
- add your db to .gitignore

### Setting Up your Virtual Environment

- source leaf/bin/activate

### Tesla Twitter Run @ 5:00 pm daily
### stock api run following day at ???

apis > spark_clean > nlp > clean > plotly

# TwitterStockAnalysis
lets git it done

## Goals

- We want to compare Stock Performance vs Twitter Sentiment of a company. We will be plotting the delta change in a stock's price vs average viewpoint of a company on Twitter (over an hour, a day?)

## Steps
- api to stream data into a dataframe. 900 requests per 15 minutes is maximum
- output df into a sqlite3 database DONE
- clean database with spark: get rid of retweets, remove punctuation, get rid of stopwords (is this done out of pyspark or within pyspark?) - Sean 
- output database from sqlite3 to a pandas dataframe for nltk - Sean 
- perform NLP sentiment analysis - (word cloud to do for tweets), and group tweets by day for analysis for positive, neutral or negative 
- set up historic stock api
- compare stock from earliest tweet to latest tweet
- perform data visualizations with plotly - 
- place code into docker and then have kubernetes run the code - Both
- stretch goal: create a dash webapp with plotly
- upload to heroku

api > pandas df > sql database > pull into pyspark df (set time frames)> clean pyspark df > load back into a new db > put (time framed) data back into a pandas df > create plotly visualizations from timeframed pandas df > python creates vizs that are then placed into a slide deck


### Setting Up your SQLite3 Database

- create a sentiment.db in your local directory
- add your db to .gitignore
- modify line 41 in gme_script and appl_script to the path of sentiment

### Setting Up your Virtual Environment

- source <venv>/bin/activate

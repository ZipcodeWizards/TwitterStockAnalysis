import pandas as pd
import sqlite3
from sqlite3 import connect
from sqlalchemy import create_engine
import numpy as np
from wordcloud import WordCloud, STOPWORDS
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from PIL import Image
from IPython.display import display


def main6():
#word clound
    def create_wordcloud(text):
        mask = np.array(Image.open("cloud.png"))
        wc = WordCloud(background_color="white",
                    mask = mask,
                    max_words=3000,
                    repeat=True)
        wc.generate(str(text))
        wc.to_file("wc.png")
        print("Word Cloud Saved Successfully")
        path="wc.png"
        display(Image.open(path))

    #line graph plot

    def line_graph():
        dftweets_2 = dftweets[dftweets['new_date'].str.match('2021-09-07')]

        dftweets_2 = dftweets_2.sort_values(by = ['time'])

        dftweets_2 = dftweets_2.reset_index(drop=True)

        dfstock[['avg_polarity',"num_of_tweets"]]=0
        print(dftweets_2)
        for index, row in dfstock['time'].iteritems():
            if index == 0:
                num_tweets = dftweets_2.time.searchsorted(str(row)+'%', side = 'right')
                avg = dftweets_2.polarity[:num_tweets].mean()
                dfstock.loc[index, 'avg_polarity'] = avg
                dfstock.loc[index, 'num_of_tweets'] = num_tweets
                last_index = index
            else:
                new_num_tweets = dftweets_2.time.searchsorted(str(row)+'%', side = 'right')
                avg = dftweets_2.polarity[num_tweets:new_num_tweets].mean()
                dfstock.loc[index, 'avg_polarity'] = avg
                dfstock.loc[index, 'num_of_tweets'] = new_num_tweets-num_tweets
                num_tweets = new_num_tweets

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=dfstock['time'], y=dfstock['last'], name="Stock Price"),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=dfstock['time'], y=dfstock['avg_polarity'], name="Polarity"),
            secondary_y=True,
        )
        # Add figure title
        fig.update_layout(
            title_text="Stock and Tweet Sentiment"
        )

        # Set x-axis title
        fig.update_xaxes(title_text="Time(est)")

        # Set y-axes titles
        fig.update_yaxes(title_text="Price($)", secondary_y=False)
        fig.update_yaxes(title_text="Polarity", secondary_y=True)

        fig.show()
        #print(dfstock)
    #dfstock = pd.read_csv('stock_graph.csv')
    #dftweets= pd.read_csv('data_graph.csv')
    con = sqlite3.connect('sentiment.db')
    # need select word cloud from the day before
    dftweets = pd.read_sql_query("SELECT * FROM nlp_analysis WHERE new_date LIKE '2021-09-07%'", con)   
    dfstock = pd.read_sql_query("SELECT * FROM tsla_prices_cleaned WHERE new_date LIKE '2021-09-07%'", con)  

    line_graph()
    create_wordcloud(dftweets["text"].values)
if __name__ == "__main__":
    main6()
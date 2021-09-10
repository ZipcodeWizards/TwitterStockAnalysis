import api_tesla_twitter
import api_tesla_stock
import cleaning_spark
import cleaning_nlp
import cleaning_tsla_prices
import plotly_graphs
from datetime import date
from datetime import timedelta

#as YYYT-MM-DD
today = date.today()
yesterday = today - timedelta(days = 1)
#set run_date to yesterday in moring or night use today when running main.py
run_date = yesterday

if __name__ == '__main__':
    api_tesla_twitter.main()
    cleaning_spark.main2(run_date)
    cleaning_nlp.main3(run_date)
    api_tesla_stock.main4()
    cleaning_tsla_prices.main5(run_date)
    plotly_graphs.main6(run_date)
    

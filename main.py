import tesla_twitter_api
import spark_cleaning
import NLP_cleaning



if __name__ == '__main__':
    tesla_twitter_api.tesla_twitter_run()
    spark_cleaning.spark_clean()

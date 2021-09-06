# create_df func from tesla_twitter_api

tweets = tweepy.Cursor(api.search, q="tesla", tweet_mode = "extended", lang = 'en', count = 200, include_rts= False)
        #print(cursor)
    all_tweets = []
    all_tweets.extend(tweets)

    '''for i in cursor:
        print(i.full_text)'''
    oldest_id = tweets[-1].id
    while True:
        tweets = tweepy.Cursor(api.search, q="tesla",max_id = oldest_id - 1, tweet_mode = "extended", lang = 'en', count = 200, include_rts= False)
        if len(tweets) == 0:
            break
        oldest_id = tweets[-1].id
        all_tweets.extend(tweets)
        print('N of tweets downloaded till now: {}'.format(len(all_tweets)))


    for i in all_tweets:
        screen_name.append(i.user.screen_name)
        date_time.append(i.created_at)
        text.append(i.full_text)
        retweeted.append(i.retweeted)
        lang.append(i.lang)
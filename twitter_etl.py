import tweepy 
import s3fs 
from datetime import datetime as dt
import json 
import pandas as pd 

def twitter_etl():
    consumer_key = 'e8wmYhypcdXALvhzGMTPaWa5i'
    consumer_secret = 'Mc5xFRcUoe1Zt4tko97pwE5lPcNcBI82LvNovIscadpCx25gn3'
    access_key = '948455717538844673-0E1SJiVvatF3uY9P7MSzgPDM92SOEl3'
    access_secret = 'MI10DXn3Ge0Gwg4pO1DJqhoAEx36JB46jRw0vTx0Z7uJU'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name = '@elonmusk',
                            count = 200,
                            include_rts = False,
                            tweet_mode = 'extended')

    tweet_list =[]
    for tweet in tweets:
        text = tweet._json['full_text']
        refined_tweet = {'user':tweet.user.screen_name,
                        'text':text,
                        'favorite_count':tweet.favorite_count,
                        'retweet_count':tweet.retweet_count,
                        'created_at':tweet.created_at}
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('s3://airflow-twitter-bucket-usama/elonmusk_twitter_data.csv')
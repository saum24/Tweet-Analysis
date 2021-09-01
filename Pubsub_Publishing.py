#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import tweepy
import json
import logging
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future


#Getting twitter auth credentials
f = open('twitter-api-keys.json',)
twitter_cred = json.load(f)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "egen-project-dev-private.json"


class PublishToPubsub(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.project_id = 'egen-project-dev'
        self.topic_id = 'twitter-sentiment-analysis'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []



    def get_callback(self, publish_future: Future ,data:str) -> callable:
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout = 60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")
        return callback
    
    def PublishToTopic(self, message: str):
        publish_future = self.publisher_client.publish(self.topic_path,json.dumps(message).encode("utf-8"))
        publish_future.add_done_callback(self.get_callback(publish_future,message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when = futures.ALL_COMPLETED)
        
    def on_status(self, message):
        self.PublishToTopic(message._json)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            print("Twitter maximum api hit rate reached")
            return False

def send_tweets():
    print("Entered send_tweets")
    auth = tweepy.OAuthHandler(twitter_cred['Api_key'], twitter_cred['Api_secret_key'])
    auth.set_access_token(twitter_cred['Access_token'], twitter_cred['Access_token_secret'])
        
    stream = tweepy.Stream(auth=auth, listener=PublishToPubsub())
    stream.filter(track=["us troops","us troops withdrawal"], languages=['en'])

if __name__ == "__main__":
    
    
  
    send_tweets()


# In[ ]:





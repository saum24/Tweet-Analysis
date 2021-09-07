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


#Loading credentials
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



    def get_callback(self, publish_future: Future ,data:str) -> callable:    #Waiting for the acknowdledgment for 60 sec
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout = 60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")
        return callback
    
    def PublishToTopic(self, message):      #Publishing the message to pubsub topic
        #print(message, '\n')
        #publishing and saving the acknowdledgment
        publish_future = self.publisher_client.publish(self.topic_path,json.dumps(message).encode("utf-8"))
        #calling the callback function with message and message id
        publish_future.add_done_callback(self.get_callback(publish_future,message))
        #appending the acknowdledgment
        self.publish_futures.append(publish_future)
        #waiting for the publishing to complete
        futures.wait(self.publish_futures, return_when = futures.ALL_COMPLETED)
        
    def on_status(self, message):                #Tweet downloaded
        self.PublishToTopic(message._json)
        return True

    def on_error(self, status_code):             #error handling on exceeding attempt limit in a window of time to connect to streaming API
        if status_code == 420:
            print("Twitter API connection attempt limit reached")
            return False

def send_tweets():
    print("Entered send_tweets")
    
    #Create the authentication object
    auth = tweepy.OAuthHandler(twitter_cred['Api_key'], twitter_cred['Api_secret_key'])
    #Set the access token and the access token secret
    auth.set_access_token(twitter_cred['Access_token'], twitter_cred['Access_token_secret'])    
    #Stream Listening
    stream = tweepy.Stream(auth=auth, listener=PublishToPubsub())
    #Search list
    stream.filter(track=["us troops","us troops withdrawal"], languages=['en'])

if __name__ == "__main__":                      #main function 
      
    send_tweets()


# In[ ]:





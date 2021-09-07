#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Importing all the necessary packages and operators
import os
import pandas as pd
import dask.dataframe as dd
import json
import datetime
from textblob import TextBlob
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


#Defining the dataset and the table
DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'Twitter_data')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'US_troop')

#Defining the dag
dag = models.DAG(
    dag_id='storage_to_bq',
    start_date=days_ago(1),
    schedule_interval='@daily'
    
)
 
    
def sentiment_calc(text):                          #Calculating the polarity, based on the tweet text
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None
    
    
# Function to merge all the csv files into a single file and perform necessary transformations
def transform_data():
    #Read .CSV files into dask dataframe
    dask_df = dd.read_csv('gs://egen_bucket01/twitter_message/*.csv', dtype={'Location': 'object','Place': 'object'})
    #saving the the data in a dataframe 
    df = dask_df.compute()


    #Transforming the time column
    df['Tweet_time'] = pd.to_datetime(df['Tweet_time'])
    df['Tweet_time'] = pd.to_datetime(df["Tweet_time"].dt.strftime("%m-%d-%Y %H:%M:%S"))
   
    #Cleaning tweet_text, user_name and tweet_source columns
    #Transforming and removing unwanted characters from the user_name column
    User_name = df['User_name']
    User_name = User_name.str.lower()
    User_name = User_name.str.replace(r'[^A-Za-z0-9 ]', '').replace(r'[^\x00-\x7F]+', '')
    User_name = User_name.str.replace('#', '').replace('_', ' ').replace('@', '')
    df['User_name'] = User_name
    
    #Transforming and removing unwanted characters from the text column
    tweet_string= df['text']
    tweet_string = tweet_string.str.lower()
    tweet_string = tweet_string.str.replace(r'[^A-Za-z0-9 ]', '').replace(r'[^\x00-\x7F]+', '') 
    tweet_string = tweet_string.str.replace(r'https?://[^\s<>"]+|www\.[^\s<>"]+', "")
    tweet_string = tweet_string.str.replace('#', '').replace('_', ' ').replace('@', '')
    df['text'] = tweet_string
    
    # Extacting text from HTML tag
    tweet_source = df['Tweet_source']
    tweet_source = tweet_source.str.extract(r'<.+>([\w\s]+)<.+>', expand = False)
    df['Tweet_source'] = tweet_source

    
    #Performing Sentiment Analysis of tweets
    df['sentiment_polarity'] = df['text'].apply(sentiment_calc)
    
    #BAsed on the polarity, finding the sentiment and dropping the polarity column
    df.loc[df['sentiment_polarity'] > 0, 'Sentiment'] = "Positive"
    df.loc[df['sentiment_polarity'] == 0, 'Sentiment'] = "Neutral"
    df.loc[df['sentiment_polarity'] < 0, 'Sentiment'] = "Negative"
    df.drop("sentiment_polarity", axis=1, inplace= True)

    #Saving the merged and transformed csv in the bucket
    df.to_csv('gs://egen_bucket01/cleaned_messages/transformed.csv', index=False)

    
    
# Calling the transform function into a Python operator
transform_csv = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_data,
    dag=dag
    )


# Create a dataset in BigQuery
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bq_dataset', dataset_id=DATASET_NAME, dag=dag
)


# Loading the transformed csv file into BQ Table
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='egen_bucket01',
    source_objects=['cleaned_messages/transformed.csv'],
    destination_project_dataset_table= f"{DATASET_NAME}.{TABLE_NAME}",
    
    #defining the schema of the table
    schema_fields=[

        {'name': 'Tweet_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Tweet_time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'Tweet_source', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'User_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'User_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Place', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'User_follower_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'User_friend_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Tweet_text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Tweet_Sentiment', 'type': 'STRING', 'mode': 'NULLABLE'}],

    #Truncating and loading the data in the table
    write_disposition='WRITE_TRUNCATE',
    #Skipping the 1st row (column names) of the tranformed csv
    skip_leading_rows = 1,
    dag=dag
)


# Setting up the task sequence for the DAG
transform_csv >> create_dataset >> load_csv


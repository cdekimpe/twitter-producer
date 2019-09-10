#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import json
import twitter  # pip install twitter
from kafka import KafkaProducer # pip install kafka-python
from confluent_kafka import avro # pip install confluent-kafka
from confluent_kafka.avro import AvroProducer


def main():

    # Put here your Twitter API credentials obtained at https://apps.twitter.com/
    # Note: you need a Twitter account to create an app.
    with open("../twitterOAuth.json") as file:
        OAJson = json.load(file)
    oauth = twitter.OAuth(OAJson["token"], OAJson["token_secret"], OAJson["consumer_key"], OAJson["consumer_secret"])
    t = twitter.TwitterStream(auth=oauth)

    key_schema = avro.load('schema/key.avsc')
    value_schema = avro.load('schema/value.avsc')

    avroProducer = AvroProducer(
        {'bootstrap.servers': 'kafka1.architect.data:9092,kafka2.architect.data:9092,kafka3.architect.data:9092'},
        default_key_schema=key_schema, default_value_schema=value_schema)

    sample_tweets_in_english = t.statuses.sample(language="en")
    for tweet in sample_tweets_in_english:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        # Tweet text
        date = tweet['created_at'].split(' ')
        key = {
            'date': date[5] + "-" + date[1] + "-" + date[2] + "-" + date[3].split(':')[0]
        }
        value = {
            'text': tweet['text'],
            'date': tweet['created_at'],
            'hashtags': [h['text'] for h in tweet["entities"]["hashtags"]]
        }
        avroProducer.produce(topic='avro', key=key, value=value, key_schema=key_schema, value_schema=value_schema)
        producer.send('tweets', product)

    avroProducer.flush(10)
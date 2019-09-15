#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import json
import twitter  # pip install twitter
import datetime
import calendar
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
        {'bootstrap.servers': 'confluent-kafka.architect.data:9092', 'schema.registry.url': 'http://localhost:8081'},
        default_key_schema=key_schema, default_value_schema=value_schema)

    sample_tweets_in_english = t.statuses.sample(language="en")
    for tweet in sample_tweets_in_english:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        # Tweet text
        value = {
            'date': tweet['created_at'],
            'timestamp': calendar.timegm(datetime.datetime.strptime(tweet['created_at'], "%a %b %d %X %z %Y").utctimetuple()),
            'text': tweet['text'],
            'hashtags': [h['text'] for h in tweet["entities"]["hashtags"]]
        }
        avroProducer.produce(topic='tweets', key=int(tweet['id_str']), value=value, key_schema=key_schema, value_schema=value_schema)
        avroProducer.flush(10)


if __name__ == "__main__":
    main()
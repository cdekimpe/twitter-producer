#! /usr/bin/env python
# -*- coding: utf-8 -*-
import io
import json
import twitter  # pip install twitter
from kafka import KafkaProducer # pip install kafka


def main():
    # Put here your Twitter API credentials obtained at https://apps.twitter.com/
    # Note: you need a Twitter account to create an app.

    with open("../twitterOAuth.json") as file:
        OAJson = json.load(file)
    oauth = twitter.OAuth(OAJson["token"], OAJson["token_secret"], OAJson["consumer_key"], OAJson["consumer_secret"])
    t = twitter.TwitterStream(auth=oauth)

    producer = KafkaProducer(
        bootstrap_servers=['kafka1.architect.data:9092', 'kafka2.architect.data:9092', 'kafka3.architect.data:9092'],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))

    sample_tweets_in_english = t.statuses.sample(language="en")
    for tweet in sample_tweets_in_english:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        # Tweet text
        product = {
            'text': tweet['text'],
            'hashtags': [h['text'] for h in tweet["entities"]["hashtags"]]
        }
        producer.send('tweets-2', product)

if __name__ == "__main__":
    main()

#! /usr/bin/env python
# -*- coding: utf-8 -*-
import io
import json
import twitter  # pip install twitter
#import avro.schema
#import avro.io
from kafka import KafkaProducer


def main():
    # Put here your Twitter API credentials obtained at https://apps.twitter.com/
    # Note: you need a Twitter account to create an app.

    with open("../twitterOAuth.json") as file:
        OAJson = json.load(file)
    oauth = twitter.OAuth(OAJson["token"], OAJson["token_secret"], OAJson["consumer_key"], OAJson["consumer_secret"])
    t = twitter.TwitterStream(auth=oauth)

    producer = KafkaProducer(
        bootstrap_servers=['kafka1.architect.data:9092', 'kafka2.architect.data:9092', 'kafka3.architect.data:9092'],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False))

    # Schema Avro
    #schema = avro.schema.Parse(json.dumps({
    #    "name": "Tweet",
    #    "namespace": "me.dekimpe",
    #    "type": "record",
    #    "fields": [
    #        {"name": "text", "type": "string"},
    #        {"name": "hashtags", "type": {
    #            "type": "array",
    #            "items": "string"
    #        }}
    #    ]
    #}))
    #writer = avro.io.DatumWriter(schema)
    #bytesWriter = io.BytesIO()
    #encoder = avro.io.BinaryEncoder(bytesWriter)

    sample_tweets_in_english = t.statuses.sample(language="en")
    for tweet in sample_tweets_in_english:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        # Tweet text
        product = {}
        product['text'] = tweet['text']
        product['hashtags'] = [h['text'] for h in tweet["entities"]["hashtags"]]
        productJson = json.dumps(product, ensure_ascii=False)
        print(productJson)
        producer.send(productJson, "tweets-2")

    #rawBytes = bytesWriter.getvalue()
    #producer.send(rawBytes, 'tweets-1')
    #print(rawBytes)


if __name__ == "__main__":
    main()

#! /usr/bin/env python
# -*- coding: utf-8 -*-
import io
import json
import twitter  # pip install twitter
import avro.schema
import avro.io
from kafka import KafkaProducer


def main():
    # Put here your Twitter API credentials obtained at https://apps.twitter.com/
    # Note: you need a Twitter account to create an app.
    with open("../twitterOAuth.json") as file:
        OAJson = json.load(file)
    oauth = twitter.OAuth(OAJson["token"], OAJson["token_secret"], OAJson["consumer_key"], OAJson["consumer_secret"])
    t = twitter.TwitterStream(auth=oauth)

    # Schema Avro


    schema = avro.schema.Parse(json.dumps({
        "name": "Tweet",
        "namespace": "me.dekimpe",
        "type": "record",
        "fields": [
            {"name": "text", "type": "string"},
            {"name": "hashtags", "type": {
                "type": "array",
                "items": ["string", "null"]
            }}
        ]
    }))
    writer = avro.io.DatumWriter(schema)
    bytesWriter = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytesWriter)

    #producer = KafkaProducer(
    #    bootstrap_servers=['kafka1.architect.data:9092', 'kafka2.architect.data:9092', 'kafka3.architect.data:9092'],
    #    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    sample_tweets_in_english = t.statuses.sample(language="en")
    u = 0
    for tweet in sample_tweets_in_english:
        if "delete" in tweet:
            # Deleted tweet events do not have any associated text
            continue

        print("===================================")

        # Tweet text
        test = {
            'text': tweet["text"],
            'hashtags': tweet["entities"]["hashtags"]["text"]
        }
        writer.write(test, encoder)
        u = u + 1
        if (u > 100):
            break

        #print(tweet["text"])

        # Collect hashtags
        #hashtags = [h['text'] for h in tweet["entities"]["hashtags"]]
        #if len(hashtags) > 0:
        #    print(hashtags)

    raw_bytes = bytesWriter.getvalue()
    print(len(raw_bytes))
    print(type(raw_bytes))

if __name__ == "__main__":
    main()

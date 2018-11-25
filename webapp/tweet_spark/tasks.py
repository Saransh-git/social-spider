import json
from datetime import datetime

from kafka import KafkaConsumer
from django.conf import settings
from celery.exceptions import SoftTimeLimitExceeded


def push_tweet_data_to_hive():
    consumer = KafkaConsumer(
        settings.TWEET_DATA_KAFKA_QUEUE, group_id='hive_f',auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}']
    )
    tweet_datalist = []
    # hive logic here
    for msg in consumer:
        try:
            value = json.loads(msg.value.decode()) #decoding json of value of kafka message
            if value['tweet_hashtag'] == '':       # if hashtag is not empty then fill the values in list
                continue
            value['tweet_timestamp'] = datetime.utcfromtimestamp(value['tweet_timestamp']/1000)
            tweet_datalist.append(value)
        except SoftTimeLimitExceeded:
            raise
    consumer.close()

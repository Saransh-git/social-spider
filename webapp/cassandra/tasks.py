import json

from celery import task
from django.conf import settings

from utils.kafka import Client as KafkaClient


@task
def push_tweet_data_to_kafka():
    client = KafkaClient().client
    topic = client.topics[settings.TWEET_DATA_KAFKA_QUEUE.encode()]
    cons = topic.get_balanced_consumer(
        consumer_group='cassandra_consumer', auto_commit_enable=True,
        auto_commit_interval_ms=2 * 1000,
        zookeeper_hosts=f'{settings.ZOOKEEPER_HOST}:{settings.ZOOKEEPER_PORT}'
    )
    data_list = []
    while True:
        msg = cons.consume(block=False)
        if not msg:
            break
        data_list.append(json.loads(msg))
    # cassandra push logic here

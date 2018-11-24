import json
import logging
from celery import task
from django.conf import settings
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from utils.kafka import Client as KafkaClient


logger = logging.getLogger(__name__)


@task
def push_tweet_data_to_kafka():
    client = KafkaClient().client
    topic = client.topics[settings.TWEET_DATA_KAFKA_QUEUE.encode()]
    cons = topic.get_balanced_consumer(
        consumer_group='cassandra_consumer', auto_commit_enable=True,
        auto_commit_interval_ms=2 * 1000,
        zookeeper_hosts=f'{settings.ZOOKEEPER_HOST}:{settings.ZOOKEEPER_PORT}'
    )
    auth_provider = PlainTextAuthProvider(username=settings.CASSANDRA_USERNAME, password=settings.CASSANDRA_PASSWORD)
    # authentication
    with Cluster(settings.CASSANDRA_HOST, auth_provider=auth_provider) as cluster:  # instantiate cluster with argument
        # as node of cassandra and authentication provider instance
        session = cluster.connect("twitter_data")
        while True:
            msg = cons.consume(block=False)
            if not msg:
                break
            value = json.loads(msg.value.decode())
            if value['tweet_hashtag'] == '':
                continue
            try:
                session.execute(f"insert into test_twitter_datatable json '{json.dumps(value)}';")
            except Exception as e:
                logger.error(e)

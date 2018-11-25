import json
import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from celery import task, Task
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


auth_provider = PlainTextAuthProvider(username=settings.CASSANDRA_USERNAME,
                                          password=settings.CASSANDRA_PASSWORD)  # authentication


class CusomTimeLimitMixinTask(Task):
    soft_time_limit = 100
    time_limit = 155


@task(base=CusomTimeLimitMixinTask)
def push_tweet_data_to_cassandra():
    consumer = KafkaConsumer(
        settings.TWEET_DATA_KAFKA_QUEUE, group_id='cassandra_f', auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}']
    )
    with Cluster(settings.CASSANDRA_HOST, auth_provider=auth_provider, executor_threads=10) as cluster:
        # node of cassandra and authentication provider instance
        with cluster.connect("twitter_data") as session:
            try:
                for msg in consumer:
                    try:
                        value = json.loads(msg.value.decode())
                        if value['tweet_hashtag'] == '':
                            continue
                        value['tweet_text'] = value['tweet_text'].replace("\'", " ")
                        value['tweet_text'] = value['tweet_text'].replace(",", " ")
                        value['tweet_text'] = value['tweet_text'].replace("\"", " ")

                        value['tweet_user_name'] = value['tweet_user_name'].replace("\'", " ")
                        value['tweet_user_name'] = value['tweet_user_name'].replace(",", " ")
                        value['tweet_user_name'] = value['tweet_user_name'].replace("\"", " ")
                        session.execute(f"insert into test_usermention json '{json.dumps(value)}';")
                    except SoftTimeLimitExceeded:
                        raise
                    except Exception as e:
                        print(e)
            except SoftTimeLimitExceeded:
                pass
            finally:
                consumer.close()

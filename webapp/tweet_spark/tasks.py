import json
import textwrap
import time
from datetime import datetime

from celery import task
from kafka import KafkaConsumer
from django.conf import settings
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import HTTPError

from tweet_spark.jobs import MIGRATE_CASSANDRA_DATA_TO_HADOOP
from utils.livy import LivyClient


@task(soft_time_limit=300, time_limit=550)
def push_tweet_data_to_hadoop():
    consumer = KafkaConsumer(
        settings.TWEET_DATA_KAFKA_QUEUE, group_id='push_data_to_hadoop', auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}']
    )
    try:
        tweet_datalist = []
        # hive logic here
        for msg in consumer:
            try:
                value = json.loads(msg.value.decode())  # decoding json of value of kafka message
                if value['tweet_hashtag'] == '':  # if hashtag is not empty then fill the values in list
                    continue
                value['tweet_timestamp'] = datetime.utcfromtimestamp(value['tweet_timestamp'] / 1000)
                tweet_datalist.append(value)
            except SoftTimeLimitExceeded:  # if we incur soft time limit here, it's probably time we should proceed
                # sending data to Hadoop
                raise
    except SoftTimeLimitExceeded:
        pass
    finally:
        consumer.close()

    post_data_to_spark_via_livy(tweet_datalist)


@task
def clear_idle_livy_session():
    livy_client = LivyClient()
    livy_client.delete_current_session()


def post_data_to_spark_via_livy(tweet_datalist):
    livy_client = LivyClient()
    session_url, is_created = livy_client.fetch_spark_livy_session_url()
    if is_created:
        print("Wait for 30 seconds, I will be right back!! Let the session start meanwhile.")
        time.sleep(30)  # let the session start and come to idle state
        try:
            livy_client.execute_statement_async(session_url, {'code': textwrap.dedent(
                """
                import time
                time.sleep(5)
                """
            )})
        except HTTPError:
            print("I survived once..Sleeping more 20 seconds")
            time.sleep(20)
    print("Data list length: {}".format(len(tweet_datalist)))
    index = 30

    while tweet_datalist:
        print("Let spark do it's work!!")
        time.sleep(5)
        rdd_input = tweet_datalist[:index*1000]
        tweet_datalist = tweet_datalist[index*1000:]
        statement_data = MIGRATE_CASSANDRA_DATA_TO_HADOOP.format(
            rdd_input=rdd_input,
            schema_text=textwrap.dedent(
                """
                schema = StructType([
                    StructField('tweet_text', StringType(), True),
                    StructField('tweet_id', LongType(), True),
                    StructField('tweet_user_id', LongType(), True),
                    StructField('tweet_user_name', StringType(), True),
                    StructField('tweet_hashtag', StringType(), True),
                    StructField('tweet_usermention', StringType(), True),
                    StructField('tweet_timestamp', TimestampType(), True)
                ])
                """
            ),
            tweet_data_hdfs_path="hdfs:///user/maria_dev/tweet_usermention"
        )
        try:
            livy_client.execute_statement_async(session_url, {'code': statement_data})
        except HTTPError as e:
            print(e)
        clear_idle_livy_session.delay(countdown=3*60)  # attempt to clear this session after 3 minutes

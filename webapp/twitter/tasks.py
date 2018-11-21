import json

from celery import task

from utils.kafka import Client

@task
def send_tweet_data_to_kafka(tweet_data):
    Client().send_async(b'test', json.dumps(tweet_data).encode())


def replicate_tweet_data_for_hashtags_and_user_mentions(tweet_data: dict):
    tweet_data_list = []

    hashtags = tweet_data['tweet_hashtags'] or ''
    user_mentions = tweet_data['tweet_usermentions'] or ''

    if hashtags != '':
        for hashtag in hashtags:
            if user_mentions != '':
                for user_mention in user_mentions:
                    tweet_data_list.append({
                        'tweet_text': tweet_data['tweet_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'tweet_user_id': tweet_data['tweet_user_id'],
                        'tweet_user_name': tweet_data['tweet_user_name'],
                        'tweet_hashtag': hashtag,
                        'tweet_usermention': user_mention,
                        'tweet_timestamp': tweet_data['tweet_timestamp']
                    })
            else:
                tweet_data_list.append({
                    'tweet_text': tweet_data['tweet_text'],
                    'tweet_id': tweet_data['tweet_id'],
                    'tweet_user_id': tweet_data['tweet_user_id'],
                    'tweet_user_name': tweet_data['tweet_user_name'],
                    'tweet_hashtag': hashtag,
                    'tweet_usermention': '',
                    'tweet_timestamp': tweet_data['tweet_timestamp']
                })
    else:
        if user_mentions != '':
            for user_mention in user_mentions:
                tweet_data_list.append({
                    'tweet_text': tweet_data['tweet_text'],
                    'tweet_id': tweet_data['tweet_id'],
                    'tweet_user_id': tweet_data['tweet_user_id'],
                    'tweet_user_name': tweet_data['tweet_user_name'],
                    'tweet_hashtag': '',
                    'tweet_usermention': user_mention,
                    'tweet_timestamp': tweet_data['tweet_timestamp']
                })
        else:
            tweet_data_list.append({
                'tweet_text': tweet_data['tweet_text'],
                'tweet_id': tweet_data['tweet_id'],
                'tweet_user_id': tweet_data['tweet_user_id'],
                'tweet_user_name': tweet_data['tweet_user_name'],
                'tweet_hashtag': '',
                'tweet_usermention': '',
                'tweet_timestamp': tweet_data['tweet_timestamp']
            })
    return tweet_data_list


@task
def tweet_preprocessing(tweet_data):
    """
    The tweet data will be preprocesed and its relevant fields will be extracted here.
    :param tweet_data: line returned by twitter streaming API
    :return:
    """

    if tweet_data["lang"] == "en":
        if tweet_data["text"].startswith("RT"):
            if tweet_data["retweeted_status"].get("extended_tweet"):
                tweet_text = tweet_data["retweeted_status"]["extended_tweet"]['full_text']
            else:
                tweet_text = tweet_data["retweeted_status"]["text"]

        elif tweet_data["is_quote_status"]:
            if tweet_data["quoted_status"].get("extended_tweet"):
                tweet_text = tweet_data["quoted_status"]["extended_tweet"]['full_text']
            else:
                tweet_text = tweet_data["quoted_status"]["text"]

        else:
            if tweet_data.get("extended_tweet"):
                tweet_text = tweet_data["extended_tweet"]['full_text']
            else:
                tweet_text = tweet_data["text"]

        tweet_id = tweet_data["id"]
        tweet_user_id = tweet_data["user"]['id']
        tweet_user_name = tweet_data["user"]['name']
        tweet_hashtags = [item["text"] for item in tweet_data["entities"]['hashtags']]
        tweet_usermentions = [item["screen_name"] for item in tweet_data["entities"]['user_mentions']]
        tweet_timestamp = tweet_data["timestamp_ms"]  # convert in utc if required for cassandra/hbase

        tweet_data_list = replicate_tweet_data_for_hashtags_and_user_mentions({
            'tweet_text': tweet_text,
            'tweet_id': tweet_id,
            'tweet_user_id': tweet_user_id,
            'tweet_user_name': tweet_user_name,
            'tweet_hashtags': tweet_hashtags,
            'tweet_usermentions': tweet_usermentions,
            'tweet_timestamp': tweet_timestamp
        })

        for tweet_data in tweet_data_list:
            send_tweet_data_to_kafka.delay(tweet_data)



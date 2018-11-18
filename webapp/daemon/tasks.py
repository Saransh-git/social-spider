import json

import requests

from twitter.task import tweet_preprocessing
from celery import task

from utils.oauth import generate_oauth_authorization_header


@task
def call_twitter_stream():
    url = "https://stream.twitter.com/1.1/statuses/filter.json"
    data = {'track': '@Amazon'}
    r = requests.post(url, data=data, headers={'Authorization': generate_oauth_authorization_header(url, 'POST', data)},
                      stream=True)

    for line in r.iter_lines():
        # filter out keep-alive new lines
        if line:
            decoded_line = line.decode('utf-8')
            tweet_preprocessing.delay(json.loads(decoded_line))


call_twitter_stream()


# handle exceptions
# Log exception: look how to place loggers inside a celery task/ normal python logger






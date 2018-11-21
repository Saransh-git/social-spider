import json
from json import JSONDecodeError

import requests

from twitter.tasks import tweet_preprocessing
from celery import task, Task

from utils.oauth import generate_oauth_authorization_header


class DaemonTask(Task):
    time_limit = None
    soft_time_limit = None  # explicitly set these limits so that the process doesn't stop.


@task(base=DaemonTask)
def call_twitter_stream():
    url = "https://stream.twitter.com/1.1/statuses/filter.json"
    data = {'track': '@Amazon,@Walmart,@Google'}
    r = requests.post(url, data=data, headers={'Authorization': generate_oauth_authorization_header(url, 'POST', data)},
                      stream=True)

    for line in r.iter_lines():
        # filter out keep-alive new lines
        if line:
            decoded_line = line.decode('utf-8')
            try:
                tweet_preprocessing.delay(json.loads(decoded_line))
            except JSONDecodeError:
                pass  # never mind, this might be a system message

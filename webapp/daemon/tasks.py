import json
from json import JSONDecodeError

import requests
from requests import HTTPError

from twitter.tasks import tweet_preprocessing
from celery import task, Task

from utils.oauth import generate_oauth_authorization_header


class DaemonTask(Task):
    time_limit = 1296180
    soft_time_limit = 1296000  # explicitly set these limits so that the process doesn't stop.
    default_retry_delay = 5 * 60
    max_retries = 5


@task(base=DaemonTask)
def call_twitter_stream():
    url = "https://stream.twitter.com/1.1/statuses/filter.json"
    data = {'track': '@Amazon,@Walmart,@Google'}
    r = requests.post(url, data=data, headers={'Authorization': generate_oauth_authorization_header(url, 'POST', data)},
                      stream=True)
    try:
        r.raise_for_status()
    except HTTPError:
        raise call_twitter_stream.retry()  # this would wait for 5 minutes and start again, max_retries = 5
    else:
        for line in r.iter_lines():
            # filter out keep-alive new lines
            if line:
                decoded_line = line.decode('utf-8')
                try:
                    tweet_preprocessing.delay(json.loads(decoded_line))
                except JSONDecodeError:
                    pass  # never mind, this might be a system message
        raise call_twitter_stream.retry()  # this would wait for 5 minutes and start again, max_retries = 5

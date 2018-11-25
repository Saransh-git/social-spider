import re

from celery.schedules import crontab

from webapp.settings.settings_consts import REDIS_HOST

CELERY_BROKER_URL = f'redis://{REDIS_HOST}/0'
CELERY_RESULT_BACKEND = f'redis://{REDIS_HOST}/0'
CELERY_TASK_SOFT_TIME_LIMIT = 100
CELERY_TASK_TIME_LIMIT = 120
CELERY_WORKER_MAX_TASKS_PER_CHILD = 500

CELERY_TASK_ROUTES = {
    # routes are followed in order
    'daemon.tasks.*': 'daemon',
    re.compile(r'(spark|tweet_cassandra)\.tasks\..*'): 'dataeng',
    '*': 'default'
}

CELERY_BEAT_SCHEDULE = {
    'ingest-tweet-data-to-cassandra': {
        'task': 'tweet_cassandra.tasks.push_tweet_data_to_cassandra',
        'schedule': crontab(minute='*/2')
    }
}

from celery.schedules import crontab

CELERY_BROKER_URL = 'redis://127.0.0.1:6379/0'
CELERY_RESULT_BACKEND = 'redis://127.0.0.1:6379/0'
CELERY_TASK_SOFT_TIME_LIMIT = 100
CELERY_TASK_TIME_LIMIT = 120
CELERY_WORKER_MAX_TASKS_PER_CHILD = 500

CELERY_TASK_ROUTES = {
    # routes are followed in order
    'daemon.tasks.*': 'daemon',
    '*': 'default'

}

CELERY_BEAT_SCHEDULE = {

}

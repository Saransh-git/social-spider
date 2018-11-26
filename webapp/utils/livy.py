import json

import requests
from django.conf import settings
from requests import HTTPError
from rest_framework.status import HTTP_404_NOT_FOUND

from utils.redis import rredis as redis


class LivyClient:
    """
    Wrapper around making Rest APIs to HDP spark livy server.
    """
    def __init__(self):
        self.headers = {'Content-Type': 'application/json', 'X-Requested-By': 'admin'}
        self.url = f'{settings.LIVY_HOST}:{settings.LIVY_PORT}'

    def create_new_spark_livy_session(self):
        data = {'kind': 'pyspark'}
        r = requests.post(f'{settings.LIVY_HOST}:{settings.LIVY_PORT}' + '/sessions', data=json.dumps(data),
                          headers=self.headers)
        r.raise_for_status()
        if r.json().get('state') != 'starting':
            raise Exception(f'Unable to start a new session!!  status={r.json().get("state")}')
        active_livy_session_id = r.json().get('id')
        redis.set('active_livy_session_id', active_livy_session_id)
        return f'{settings.LIVY_HOST}:{settings.LIVY_PORT}/sessions/{active_livy_session_id}'

    def fetch_spark_livy_session_url(self):
        """

        :return: (session_url, is_created)
        """
        active_livy_session_id = redis.get('active_livy_session_id')

        if not active_livy_session_id:
            return self.create_new_spark_livy_session(), True
        else:
            active_livy_session_id = active_livy_session_id.decode()
            r = requests.get(f'{self.url}/sessions/{active_livy_session_id}/state', headers=self.headers)
            if r.status_code == HTTP_404_NOT_FOUND:
                return self.create_new_spark_livy_session(), True
            r.raise_for_status()
            if r.json().get('state') in ['shutting_down', 'error', 'dead', 'success', 'not_started']:
                # Refer: https://livy.incubator.apache.org/docs/latest/rest-api.html#session
                requests.delete(f'{self.url}/sessions/{active_livy_session_id}', headers=self.headers)
                return self.create_new_spark_livy_session(), True
            else:
                return f'{settings.LIVY_HOST}:{settings.LIVY_PORT}/sessions/{active_livy_session_id}', False

    def execute_statement_async(self, session_url, statement_data):
        """
        :param session_url:
        :param statement_data: {'code': <statement_code>}  # Refer: https://livy.incubator.apache.org/examples/
        :return:
        """
        statements_url = f'{session_url}/statements'
        r = requests.post(statements_url, data=json.dumps(statement_data), headers=self.headers)
        r.raise_for_status()

    def delete_session(self, session_url):
        requests.delete(session_url, headers=self.headers)

    def fetch_livy_session_state(self, session_url):
        r = requests.get(f'{session_url}/state', headers=self.headers)
        r.raise_for_status()
        return r.json().get('state')

    def delete_current_session(self):
        active_livy_session_id = redis.get('active_livy_session_id')
        if not active_livy_session_id:
            return
        active_livy_session_id = active_livy_session_id.decode()
        session_url = f'{settings.LIVY_HOST}:{settings.LIVY_PORT}/sessions/{active_livy_session_id}'
        try:
            state = self.fetch_livy_session_state(session_url)
        except HTTPError as e:
            if e.response.status_code == HTTP_404_NOT_FOUND:
                redis.delete('active_livy_session_id')
            else:
                print(e)
        else:
            if state in ['idle', 'shutting_down', 'error', 'dead', 'success']:
                self.delete_session(f'{settings.LIVY_HOST}:{settings.LIVY_PORT}/sessions/{active_livy_session_id}')
                redis.delete('active_livy_session_id')

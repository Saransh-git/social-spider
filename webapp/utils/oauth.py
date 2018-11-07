from time import time
import random
import base64
import hmac
from hashlib import sha1
rand_num = random.uniform(1, 10000)
from urllib.parse import quote, urlencode


def escape(item):
    return quote(item, safe='~')


def generate_oauth_authorization_header(url, method, payload={}):
    """

    :param url: HTTP url which is being requested
    :param method: HTTP method used
    :param payload: Query Parameters for GET/ Data dict for POST
    :return: str
    """
    # TODO: Refactor this later
    dd = {
        'oauth_consumer_key': 'AeHu7b50SNmLO6em4bhwO7e1w',
        'oauth_nonce': str(random.SystemRandom().randint(0, 100000000)),
        'oauth_signature_method': 'HMAC-SHA1', 'oauth_token': '1003938452-HRf6XQyU0nIFFBFkPtOUmb54Dyl95RDmggDgSfz',
        'oauth_version': '1.0', 'oauth_timestamp': str(int(time()))
    }

    dd.update(payload)

    cc = [(escape(key), escape(dd[key])) for key in dd]
    cc = sorted(cc)
    ss = ''
    for item in cc:
        if ss:
            ss = f'{ss}&'
        ss = f'{ss}{item[0]}={item[1]}'

    base_str = f'POST&{escape(url)}&{escape(ss)}'

    consumer_secret = 'NfwVJJQWSNyvKB8dnwggQa3lOkWuy7LmN1ktSzbxyzl8MXuVyu'
    token_secret = 'VUHvBdWc0I94uRHVX4rmu1xe11OM7VcFckDc6C7dGh2h6'

    salt = f'{escape(consumer_secret)}&{escape(token_secret)}'
    signature = hmac.new(salt.encode(), base_str.encode(), digestmod=sha1)
    signature_base_encoded = base64.b64encode(signature.digest()).decode()
    dd['oauth_signature'] = signature_base_encoded

    auth_str = f'OAuth oauth_consumer_key=\"{escape(dd["oauth_consumer_key"])}\", ' \
               f'oauth_nonce=\"{escape(dd["oauth_nonce"])}\", ' \
               f'oauth_signature=\"{escape(dd["oauth_signature"])}\", ' \
               f'oauth_signature_method=\"{escape(dd["oauth_signature_method"])}\", ' \
               f'oauth_timestamp=\"{escape(dd["oauth_timestamp"])}\", ' \
               f'oauth_token=\"{escape(dd["oauth_token"])}\", ' \
               f'oauth_version="1.0"'

    return auth_str

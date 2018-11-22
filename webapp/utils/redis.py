from redis import StrictRedis
from django.conf import settings


rredis = StrictRedis(host=settings.REDIS_HOST, port=int(settings.REDIS_PORT), db=0)

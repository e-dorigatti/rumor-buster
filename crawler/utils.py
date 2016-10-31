import requests
import hashlib
import twitter
import settings
import redis
import secrets
import json
from progressbar import widgets


def get_twitter_stream():
    stream = twitter.TwitterStream(auth=twitter.OAuth(
        token=secrets.access_token, token_secret=secrets.token_secret,
        consumer_key=secrets.consumer_key, consumer_secret=secrets.consumer_secret
    ))

    return stream


def get_twitter_api():
    api = twitter.Twitter(auth=twitter.OAuth(
        token=secrets.access_token, token_secret=secrets.token_secret,
        consumer_key=secrets.consumer_key, consumer_secret=secrets.consumer_secret
    ))

    return api


class RateLimitedException(Exception):
    def __init__(self, ttl):
        self.ttl = ttl

    def __str__(self):
        return 'rate limited, time left: %d' % self.ttl


def api_call(method, *args, **kwargs):
    rate_limit_key = 'twitter-apis-rate-limited'
    rs = get_redis()

    ttl = rs.ttl(rate_limit_key)
    assert ttl != '-1'
    
    if ttl >= 0:
        raise RateLimitedException(ttl)

    cache_key = 'api-cache-%s' % hashlib.sha512(
        json.dumps({'meth': method.uriparts, 'args': args, 'kwargs': kwargs})
    ).hexdigest()
    res = rs.get(cache_key)
    if res is not None:
        return json.loads(res)

    try:
        res = method(*args, **kwargs)
        rs.set(cache_key, json.dumps(res))
        rs.expire(cache_key, 24 * 60 * 60)
        return res
    except twitter.TwitterHTTPError as exc:
        if exc.e.code == 429:
            rs.set(rate_limit_key, 1)
            rs.expire(rate_limit_key, 15 * 60)
            raise RateLimitedException(15 * 60)
        else:
            raise
    except:
        raise


def get_redis():
    rs = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
    return rs


def get_es_url(type_, id_):
    if type_ == 'tweet':
        index, doctype = settings.ES_TWEET_INDEX, settings.ES_TWEET_DOCTYPE
    elif type_ == 'user':
        index, doctype = settings.ES_USER_INDEX, settings.ES_USER_DOCTYPE
    else:
        index, doctype = type_.split('/')

    es_url = '%s/%s/%s/%s' % (settings.ES_HOST, index, doctype, id_)
    return es_url


class FlexibleDynamicMessage(widgets.DynamicMessage):
    def __init__(self, kwarg_name, label, format_defined, format_undefined):
        self.name = kwarg_name
        self.label = label
        self.format_defined = format_defined
        self.format_undefined = format_undefined

    def __call__(self, progress, data):
        value = data['dynamic_messages'].get(self.name)
        if value is None:
            return self.format_undefined.format(label=self.label)
        elif isinstance(value, dict):
            return self.format_defined.format(label=self.label, **value)
        else:
            return self.format_defined.format(label=self.label, value=value)

import requests
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

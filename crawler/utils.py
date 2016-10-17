import tweepy
import requests
import settings
import redis
import secrets
import json
from progressbar import widgets


def get_twitter_api(**api_kwargs):
    cache = api_kwargs.pop('cache', tweepy.FileCache(settings.TWEEPY_CACHE, timeout=-1))
    wait_on_rate_limit = api_kwargs.pop('wait_on_rate_limit', True)

    auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.token_secret)
    api = tweepy.API(auth, cache=cache, wait_on_rate_limit=wait_on_rate_limit, **api_kwargs)

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

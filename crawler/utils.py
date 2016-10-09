import tweepy
import requests
import settings
import redis
from neo4j.v1 import GraphDatabase, basic_auth
import secrets
import json


def get_twitter_api(**api_kwargs):
    cache = api_kwargs.pop('cache', tweepy.FileCache(settings.TWEEPY_CACHE, timeout=-1))
    wait_on_rate_limit = api_kwargs.pop('wait_on_rate_limit', True)

    auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.token_secret)
    api = tweepy.API(auth, cache=cache, wait_on_rate_limit=wait_on_rate_limit, **api_kwargs)

    return api


def get_neo4j_session():
    driver = GraphDatabase.driver(
        settings.NEO4J_URL, auth=basic_auth(secrets.neo4j_user, secrets.neo4j_password)
    )
    return driver.session()


def get_redis():
    rs = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
    return rs


def add_tweet(tweet_id, graph=None):
    g = graph or get_neo4j_session()
    try:
        g.run('MERGE(t:tweet {id: {tid}})', parameters=dict(tid=tweet_id))
    finally:
        if not graph:
            g.close()


def add_relation(id_from, id_to, label, graph=None):
    g = graph or get_neo4j_session()
    try:
        g.run(
            'MATCH (t:tweet {id: {id1}}), (u:tweet {id: {id2}}) MERGE (t)-[r:%s]->(u)' % label,
            parameters=dict(id1=id_from, id2=id_to)
        )
    finally:
        if not graph:
            g.close()


def get_es_url(type_, id_):
    if type_ == 'tweet':
        index, doctype = settings.ES_TWEET_INDEX, settings.ES_TWEET_DOCTYPE
    elif type_ == 'user':
        index, doctype = settings.ES_USER_INDEX, settings.ES_USER_DOCTYPE
    else:
        index, doctype = type_.split('/')

    es_url = '%s/%s/%s/%s' % (settings.ES_HOST, index, doctype, id_)
    return es_url

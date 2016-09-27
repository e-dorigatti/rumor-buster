from celery import Celery
import tweepy
import secrets
import settings
import requests
import json
from celery.utils.log import get_task_logger
from neo4j.v1 import GraphDatabase, basic_auth
from contextlib import closing


logger = get_task_logger(__name__)

app = Celery('tasks')
app.config_from_object('crawler.celeryconfig')


def get_twitter_api():
    auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.token_secret)
    api = tweepy.API(auth)
    return api


def get_neo4j_session():
    driver = GraphDatabase.driver(
        settings.NEO4J_URL, auth=basic_auth(secrets.neo4j_user, secrets.neo4j_password)
    )
    return driver.session()


@app.task(bind=True, rate_limit=0.1, max_retries=None)
def expand_tweet(self, tweet):
    api = get_twitter_api()

    if not isinstance(tweet, tweepy.models.Status):
        try:
            tweet = api.get_status(tweet)
        except tweepy.TweepError:
            logger.exception('error while retrieving tweet %s', tweet)
            raise self.retry(countdown=15*60)

    try:
        retweets = api.retweets(tweet.id, count=100)
    except tweepy.TweepError:
        logger.exception('error while retrieving retweets of tweet %s', tweet.id)
        raise self.retry(countdown=15*60)

    logger.info('tweet %d was retweeted %d times, but we only got %d :(',
                tweet.id, tweet.retweet_count, len(retweets))

    with closing(get_neo4j_session()) as graph:
        graph.run('MERGE(t:tweet {id: {tid}})', parameters=dict(tid=tweet.id))
        for each in retweets:
            expand_tweet.delay(each.id)
            graph.run('MERGE(t:tweet {id: {tid}})', parameters=dict(tid=each.id))
            graph.run(
                'MATCH (t:tweet {id: {id1}}), (u:tweet {id: {id2}}) MERGE (t)-[r:retweet]->(u)',
                parameters=dict(id1=tweet.id, id2=each.id)
            )

    es_url = '%s/%s/%s/%d' % (settings.ES_HOST, settings.ES_INDEX,
                              settings.ES_DOCTYPE, tweet.id)
    r = requests.put(es_url, data=json.dumps(tweet._json))
    r.raise_for_status()

    return es_url

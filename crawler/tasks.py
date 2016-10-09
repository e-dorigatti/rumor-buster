from celery import Celery
import tweepy
from celery.utils.log import get_task_logger
from contextlib import closing
from crawler import utils
import requests
import json
import progressbar
import urllib
import datetime


logger = get_task_logger(__name__)

app = Celery('tasks')
app.config_from_object('crawler.celeryconfig')


@app.task(bind=True, rate_limit=0.1, max_retries=None)
def search(self, text, count=100):
    api = utils.get_twitter_api()

    try:
        return api.search(text, rpp=count)
    except tweepy.TweepError:
        logger.exception(
            'while retrieving page %d of search query "%s"', page, text
        )
        raise self.retry(countdown=15*60)


@app.task(rate_limit=0.1)
def expand_tweet(tweet):
    api = utils.get_twitter_api()
    if not isinstance(tweet, tweepy.models.Status):
        tweet = api.get_status(tweet)

    if hasattr(tweet, 'retweeted_status'):
        utils.add_tweet(tweet)
        requests.put(
            utils.get_es_url('tweet', tweet.id_str), data=json.dumps(tweet._json)
        ).raise_for_status()
        expand_tweet.delay(tweet.retweeted_status.id_str, tweet.retweeted_status.id_str)
        return

    rts = api.retweets(tweet.id_str, count=100)
    logger.info('got %d retweets for tweet %s', len(rts), tweet.id_str)
    with closing(utils.get_neo4j_session()) as graph:
        utils.add_tweet(tweet, graph)
        requests.put(
            utils.get_es_url('tweet', tweet.id_str), data=json.dumps(tweet._json)
        ).raise_for_status()

        for each in rts:
            utils.add_tweet(each, graph)
            requests.put(
                utils.get_es_url('tweet', each.id_str), data=json.dumps(tweet._json)
            ).raise_for_status()
            utils.add_relation(tweet.id_str, each.id_str, 'retweet', graph)

    return tweet.id_str


def get_user_timeline(user):
    api = utils.get_twitter_api()

    timeline, done, max_id = [], False, None
    while not done:
        logger.debug('getting timeline for %s until %s', user, max_id)
        kwargs = dict(user_id=user, include_rts=1, count=200)
        if max_id:
            kwargs['max_id'] = str(max_id)

        res = api.user_timeline(**kwargs)

        done = not bool(res) or len(timeline) >= 3200
        max_id = min(int(t.id_str) for t in res) - 1
        timeline.extend(res)

        print len(set(t.id_str for t in timeline))

    return timeline


@app.task()
def analyze_user(user):
    timeline = get_user_timeline(user)

    retweets = len([t for t in timeline if t.retweeted])
    followers = timeline[0].user.followers_count
    followees = timeline[0].user.friends_count

    originality = (len(timeline) - retweets) / max(float(retweets), 1)
    role = float(followers) / followees

    logger.info('stats for user %s - originality %f role %f',
                str(user), originality, role)
    logger.info('more stats for %s - retweets %d followers %d followees %d',
                str(user), retweets, followers, followees)



class BaseStreamListener(app.Task, tweepy.StreamListener):
    abstract = True

    def __init__(self):
        self.api = utils.get_twitter_api(wait_on_rate_limit=False)
        self.logger = get_task_logger(self.__name__)
        self.count = 0
        self.stream = None
        self.pbar = progressbar.ProgressBar(
            max_value=progressbar.UnknownLength,
        )

    def get_query(self):
        """
        this method is called just before attaching to the stream. return the query used
        to filter the stream, or None to use the one provided when start is called
        """
        return None

    @classmethod
    def start(cls, track=None, languages=None):
        matching = [
            task for task in app.tasks.values()
            if isinstance(task, cls)
        ]
        assert len(matching) == 1 and matching[0]
        listener = matching[0]
        listener.this_task = listener

        track = listener.get_query() or track
        assert track is not None, 'forgot to set stream filter'

        listener.stream = tweepy.Stream(auth=listener.api.auth, listener=listener)
        listener.stream.filter(languages=languages or ['en'], track=track)

    def run(self, status):
        raise NotImplementedError

    def on_status(self, status):
        if self.this_task:
            self.this_task.delay(status._json)
            self.count += 1
            self.pbar.update(self.count)
        else:
            self.stream.disconnect()
            logger.info('disconnected from stream')


class SimpleStreamListener(BaseStreamListener):
    """
    given a query, listens to the stream and saves every tweet in
    es/neo4j. the source tweet of a retweet is retrieved, as well
    """
    ignore_result = True

    def __init__(self):
        BaseStreamListener.__init__(self)
        self.session = requests.Session()

    def run(self, status):
        self.logger.info('starting task')
        with closing(utils.get_neo4j_session()) as graph:
            utils.add_tweet(status['id_str'])
            self.session.put(
                utils.get_es_url('tweet', status['id_str']), data=json.dumps(status)
            ).raise_for_status()

            self.logger.debug('got tweet %s: %s', status['id_str'], status['text'])

            if 'retweeted_status' in status:
                original_id = status['retweeted_status']['id_str']
                utils.add_relation(original_id, status['id_str'], 'retweet', graph)
                utils.add_tweet(original_id, graph)

                es_url = utils.get_es_url('tweet', original_id)
                if self.session.head(es_url).status_code == 404:
                    original = self.api.get_status(original_id)

                    self.session.put(
                        utils.get_es_url('tweet', original_id), data=json.dumps(original._json)
                    ).raise_for_status()

                    self.logger.debug('fetched original tweet %s of retweet %s',
                                      original_id, status['id_str'])


class TrendingStreamListener(SimpleStreamListener):
    """
    adaptively listens to tweets on trending topics
    """

    update_interval_secs = 15 * 60

    def __init__(self):
        SimpleStreamListener.__init__(self)
        self.updated = datetime.datetime.utcnow()
        self.update_in_progress = False

    def get_query(self):
        trending = sorted(self.api.trends_place(1)[0]['trends'],
                          key=lambda t: t['tweet_volume'],
                          reverse=True)[:3]
        track = [urllib.unquote(t['query']) for t in trending]
        print 'will listen for these trending topics:', track

        return track

    def on_status(self, status):
        now = datetime.datetime.utcnow()
        if (not self.update_in_progress and
           (now - self.updated).total_seconds() > self.update_interval_secs):

            self.update_in_progress = True
            new_track = self.get_query()
            self.stream.disconnect()
            self.stream.filter(track=new_track, async=True)
            self.updated = now
            self.update_in_progress = False

        super(TrendingStreamListener, self).on_status(status)

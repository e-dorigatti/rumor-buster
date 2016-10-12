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
import logging


logger = get_task_logger(__name__)
logging.getLogger('neo4j.bolt').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)

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
        self.must_stop = False
        self.pbar = progressbar.ProgressBar(
            max_value=progressbar.UnknownLength,
            redirect_stdout=True,
        )

    def get_query(self):
        """
        this method is called just before attaching to the stream. return the query used
        to filter the stream, or None to use the one provided when start is called
        """
        return None

    @classmethod
    def start(cls, track=None, languages=None, forever=True):
        matching = [
            task for task in app.tasks.values()
            if isinstance(task, cls)
        ]
        assert len(matching) == 1 and matching[0]
        listener = matching[0]
        listener.this_task = listener

        listener.stream = tweepy.Stream(auth=listener.api.auth, listener=listener)
        while forever and not listener.must_stop:
            track = listener.get_query() or track
            assert track is not None, 'forgot to set stream filter'
            #listener.stream.filter(languages=languages or ['en'], track=track)
            listener.stream.filter(track=track)
            print 'RESET'

    def run(self, status):
        raise NotImplementedError

    def on_status(self, status):
        if self.this_task:
            self.this_task.delay(status._json)
            self.count += 2
            self.pbar.update(self.count)
        else:
            self.stream.disconnect()
            logger.info('disconnected from stream')

    def on_error(self, status_code):
        print 'got error code', status_code

    def on_disconnect(self, notice):
        print 'disconnected, notice:', notice


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
            self.stream.disconnect()

        super(TrendingStreamListener, self).on_status(status)


class InterestingStuffStreamListener(SimpleStreamListener):
    RETWEETS_SET_KEY = 'interesting-retweets-list'
    SAVED_COUNT_KEY = 'interesting-saved'
    MIN_RETWEETS_KEY = 'interesting-min-retweets'

    TOP_RETWEET_PERCENT = 15
    MAX_RETWEET_THRESHOLD = 15
    STATS_UPDATE_INTERVAL = 50  # tweets

    def __init__(self):
        SimpleStreamListener.__init__(self)
        self.redis = utils.get_redis()
        self.redis.set(self.SAVED_COUNT_KEY, 0)
        self.redis.set(self.MIN_RETWEETS_KEY, 1)
        self.update_in_progress = False

        self.pbar = progressbar.ProgressBar(
            max_value=progressbar.UnknownLength,
            redirect_stdout=True,
            widgets=[
                progressbar.RotatingMarker(), ' ',
                'Tweets: ', progressbar.Counter(), ' r - ',
                utils.FlexibleDynamicMessage(
                    kwarg_name='saved',
                    label='',
                    format_defined='{value} s',
                    format_undefined='? s',
                ), ' | ',
                progressbar.Timer(), ' | ',
                utils.FlexibleDynamicMessage(
                    kwarg_name='base',
                    label='Base',
                    format_defined='{label}: {count} [1...{max:.3g}]',
                    format_undefined='{label}: ------'
                ), ' | ',
                utils.FlexibleDynamicMessage(
                    kwarg_name='top',
                    label='Top %d%%' % self.TOP_RETWEET_PERCENT,
                    format_defined='{label}: {rts:.3g} RTs ({rounding} - {round_down:.3g}%/{round_up:.3g}%)',
                    format_undefined='{label}: ------'
                ),
            ]
        )

    def get_query(self):
        self.redis.delete(self.RETWEETS_SET_KEY)

        places = [
            1,
            23424975,   # UK
            23424977,   # USA
            23424748,   # Australia
            23424916,   # New Zealand
        ] + [place['woeid'] for place in self.api.trends_available()]

        trends = []
        try:
            for each in places:
                tts = [topic for topic in self.api.trends_place(each)[0]['trends']
                       if topic['tweet_volume']]
                trends.extend(tts)
        except tweepy.RateLimitError:
            pass

        assert trends, 'rate limited'
        trends = sorted(
            trends, key=lambda t: t['tweet_volume'], reverse=True
        )[:400]
        track = [urllib.unquote(t['query']) for t in trends]
        print 'will listen to %d trending topics' % len(trends)

        return track

    def run(self, status):
        orig_tweet_id = status.get('retweeted_status', {}).get('id_str', None)

        orig_retweets = 0
        if orig_tweet_id:
            orig_retweets = self.redis.zincrby(self.RETWEETS_SET_KEY, orig_tweet_id, 1)

        min_rts = int(self.redis.get(self.MIN_RETWEETS_KEY))
        if orig_retweets >= min_rts:
            self.redis.incr(self.SAVED_COUNT_KEY)
            SimpleStreamListener.run(self, status)
            status = 'SAVED'
        else:
            status = 'SKIPPED'

        logger.info('tweet %s retweeted %d times, threshold is %d, %s',
                    orig_tweet_id, orig_retweets, min_rts, status)

    def on_status(self, status):
        if self.count and self.count % self.STATS_UPDATE_INTERVAL == 0:
            min_rts = self.update_statistics()
            if min_rts > self.MAX_RETWEET_THRESHOLD and not self.update_in_progress:
                print 'reached threshold, resetting'
                self.update_in_progress = True
                self.stream.disconnect()

        super(InterestingStuffStreamListener, self).on_status(status)

    def update_statistics(self):
        num_tweets = self.redis.zcard(self.RETWEETS_SET_KEY)
        scores = [s for (k, s) in self.redis.zrange(self.RETWEETS_SET_KEY, 0,
                                                    num_tweets, withscores=True)]
        if not scores:
            return -1

        count = len(scores)
        top_k_pcentile = count * (1 - self.TOP_RETWEET_PERCENT / 100.0)
        num_rounded_down = sum(1 for x in scores if x >= scores[int(top_k_pcentile)])
        pcent_rounded_down = 100.0 * num_rounded_down / count

        num_rounded_up = sum(
            1 for x in scores if x >= scores[int(top_k_pcentile)] + 1
        )
        pcent_rounded_up = 100.0 * num_rounded_up / count

        # sanity check
        assert pcent_rounded_down >= self.TOP_RETWEET_PERCENT >= pcent_rounded_up, (
            pcent_rounded_down, self.TOP_RETWEET_PERCENT, pcent_rounded_up
        )

        # choose the rounding so as to be closest to the given percentage
        if self.TOP_RETWEET_PERCENT - pcent_rounded_up < pcent_rounded_down - self.TOP_RETWEET_PERCENT:
            min_rts = scores[int(top_k_pcentile)] + 1
            rounding = 'U'
        else:
            min_rts = scores[int(top_k_pcentile)]
            rounding = 'D'

        assert int(min_rts) > 0  # shouldn't happen, right?
        self.redis.set(self.MIN_RETWEETS_KEY, int(min_rts))

        # update progress bar
        saved = self.redis.get(self.SAVED_COUNT_KEY)
        self.pbar.update(
            self.count, saved=saved,
            base={
                'count': len(scores),
                'max': max(scores)
            },
            top={
                'rts': min_rts,
                'round_up': pcent_rounded_up,
                'round_down': pcent_rounded_down,
                'rounding': rounding,
            }
        )

        return min_rts

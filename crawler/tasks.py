from celery import Celery
import time
import twitter
import traceback
import random
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded
from contextlib import closing
import utils
import requests
try:
    import ujson as json
except ImportError:
    import json
import progressbar
import urllib
import datetime
import logging


logger = get_task_logger(__name__)
logging.getLogger('requests').setLevel(logging.WARN)

app = Celery('tasks')
app.config_from_object('celeryconfig')


@app.task(bind=True) #rate_limit=0.1)
def expand_tweet(self, tweet):
    api = utils.get_twitter_api()
    tweet = utils.api_call(api.statuses.show, id=tweet)

    if 'retweeted_status' in tweet:
        requests.put(
            utils.get_es_url('tweet', tweet['id_str']), data=json.dumps(tweet)
        ).raise_for_status()
        expand_tweet.delay(tweet['retweeted_status']['id_str'])
        return

    try:
        rts = utils.api_call(api.statuses.retweets, id=tweet['id_str'], count=100)
    except twitter.TwitterHTTPError as exc:
        if exc.e.code == 429:
            raise self.retry(countdown=15*60 + random.randint(1, 6*60))
        else:
            raise

    logger.info('got %d retweets for tweet %s', len(rts), tweet['id_str'])
    requests.put(
        utils.get_es_url('tweet', tweet['id_str']), data=json.dumps(tweet)
    ).raise_for_status()

    for each in rts:
        requests.put(
            utils.get_es_url('tweet', each['id_str']), data=json.dumps(tweet)
        ).raise_for_status()

    return tweet['id_str']


def get_user_timeline(user, count=3200):
    api = utils.get_twitter_api()

    count = min(count, 3200)
    done, max_id = False, None
    while not done:
        logger.info('getting timeline for %s until %s', user, max_id)
        kwargs = dict(user_id=user, include_rts=1, count=min(200, count))
        if max_id:
            kwargs['max_id'] = str(max_id)

        res = utils.api_call(api.statuses.user_timeline, **kwargs)

        done = True
        for tweet in res:
            count -= 1
            if count < 0:
                done = True
                break
            else:
                done = False
                yield tweet
            max_id = min(tweet['id_str'], max_id) if max_id else tweet['id_str']


def search(query, count=0):
    api = utils.get_twitter_api()

    retrieved, done, max_id = 0, False, None
    while not done:
        print 'getting tweets for %s until %s' % (query, max_id)
        print retrieved
        kwargs = dict(q=query, result_type='mixed', count=100, include_entities=True)
        if max_id:
            kwargs['max_id'] = str(max_id)

        res = utils.api_call(api.search.tweets, **kwargs)['statuses']
        retrieved += len(res)

        done = not bool(res) or (count > 0 and retrieved >= count)
        max_id = min(int(t['id_str']) for t in res) - 1
        for tweet in res:
            yield tweet


@app.task()
def analyze_user(user):
    timeline = list(get_user_timeline(user))

    retweets = len([t for t in timeline if t.retweeted])
    followers = timeline[0].user.followers_count
    followees = timeline[0].user.friends_count

    originality = (len(timeline) - retweets) / max(float(retweets), 1)
    role = float(followers) / followees

    logger.info('stats for user %s - originality %f role %f',
                str(user), originality, role)
    logger.info('more stats for %s - retweets %d followers %d followees %d',
                str(user), retweets, followers, followees)


@app.task(bind=True, max_retries=None)
def expand_user_of_tweets(self, tweets):
    # rationale: we don't want consumers to waste time calling
    # this task for each tweet, but, on the other hand, we don't
    # want to have long running tasks
    if isinstance(tweets, list):
        for tweet in tweets:
            expand_user_of_tweets.delay(tweet)
        return
    else:
        tweet = tweets

    tweet_es_url = utils.get_es_url('tweet', tweet)
    r = requests.get(tweet_es_url)
    if not r.ok:
        return

    user_data = r.json()['_source']['user']
    user_id = user_data['id_str']

    timeline_ids = []
    try:
        for i, tweet in enumerate(get_user_timeline(user_id, count=400)):
            if i % 4 == 0:
                SimpleStreamListener.get_task().delay(tweet, expand_rt=False)
                timeline_ids.append(tweet['id_str'])
    except SoftTimeLimitExceeded:
        pass
    except utils.RateLimitedException:
        raise self.retry(countdown=15*60 + random.randint(1, 5*60))

    user_data['timeline'] = timeline_ids
    user_es_url = utils.get_es_url('user', user_id)
    requests.put(user_es_url, data=json.dumps(user_data))


class BaseStreamListener(app.Task):
    abstract = True

    def __init__(self):
        self.stream = utils.get_twitter_stream()
        self.api = utils.get_twitter_api()
        self.logger = get_task_logger(self.__name__)
        self.count = 0
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
    def start(cls, track=None, languages=None):
        matching = [
            task for task in app.tasks.values()
            if type(task) == cls  # ignore sub-classes
        ]
        assert len(matching) == 1 and matching[0]
        listener = matching[0]
        listener.this_task = listener
 
        refresh_track = True
        retries = 0
        while True:
            if refresh_track:
                try:
                    track = listener.get_query() or track
                except:
                    traceback.print_exc()
                    print 'Sleeping 60s before refreshing track'
                    time.sleep(60)
                    continue

                assert track is not None, 'forgot to set stream filter'
                refresh_track = False

            try:
                stream = listener.stream.statuses.filter(
                    track=track, languages=languages, timeout=90,
                )
                for msg in stream:
                    if 'hangup' in msg and msg['hangup']:
                        listener.on_connection_broken(msg)
                        retries += 1
                        rest = min(2 * 60, 2**retries)
                        print 'sleeping %ds...' % rest
                        time.sleep(rest)
                    elif 'timeout' in msg and msg['timeout']:
                        print 'Timeout'
                        break
                    else:
                        retries = 0
                        stop = listener.on_status(msg)
                        if stop == False:
                            print 'STOP'
                            refresh_track = True
                            break
            except KeyboardInterrupt:
                traceback.print_exc()
                break
            except twitter.TwitterHTTPError as exc:
                if exc.e.code == 420:
                    print exc.response_data
                    print 'sleeping for 5m...'
                    time.sleep(5 * 60)
                else:
                    traceback.print_exc()
            except:
                traceback.print_exc()
            print 'RESET'

    def run(self, status):
        raise NotImplementedError

    def on_status(self, status):
        self.count += 1
        self.pbar.update(self.count)
        self.this_task.delay(status)

    def on_connection_broken(self, msg):
        print 'connection broken', msg


class SimpleStreamListener(BaseStreamListener):
    """
    given a query, listens to the stream and saves every tweet in
    es. the source tweet of a retweet is retrieved, as well
    """
    ignore_result = True

    def __init__(self):
        BaseStreamListener.__init__(self)
        self.session = requests.Session()

    @staticmethod
    def get_task():
        # quite an ugly hack
        return app.tasks['tasks.SimpleStreamListener']

    def save_tweet(self, status):
        data = json.dumps(status)

        es_url = utils.get_es_url('tweet', status['id_str'])
        self.session.put(
            es_url, data=data
        ).raise_for_status()

    def run(self, status, expand_rt=False):
        self.logger.info('starting task')
        self.logger.debug('got tweet %s: %s', status['id_str'], status['text'])
        self.save_tweet(status)

        if expand_rt and 'retweeted_status' in status:
            original_id = status['retweeted_status']['id_str']

            try:
                original = utils.api_call(self.api.statuses.show, id=original_id)
            except utils.RateLimitedException as exc:
                raise self.retry(countdown=15*60 + random.randint(1, 6*60))
            
            self.save_tweet(original)
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
        trends = utils.api_call(self.api.trends.place, _id=1)
        trending = sorted(trends[0]['trends'],
                          key=lambda t: t['tweet_volume'],
                          reverse=True)[:3]
        track = [urllib.unquote(t['query']) for t in trending]
        print 'will listen for these trending topics:', track

        return ','.join(track)

    def on_status(self, status):
        now = datetime.datetime.utcnow()
        if (not self.update_in_progress and
           (now - self.updated).total_seconds() > self.update_interval_secs):
            self.update_in_progress = True
            self.stream.disconnect()

        super(TrendingStreamListener, self).on_status(status)


class InterestingStuffStreamListener(SimpleStreamListener):
    """
    Adaptively finds and stores the most retweeted trending stories
    """
    RETWEETS_SET_KEY = 'interesting-retweets-list'
    SAVED_COUNT_KEY = 'interesting-saved'
    MIN_RETWEETS_KEY = 'interesting-min-retweets'

    TOP_RETWEET_PERCENT = 15
    MAX_RETWEET_THRESHOLD = 6
    STATS_UPDATE_INTERVAL = 50  # tweets

    MAX_TIME_ON_TOPIC = 5 * 60
    
    def __init__(self):
        SimpleStreamListener.__init__(self)
        self.redis = utils.get_redis()
        self.redis.set(self.SAVED_COUNT_KEY, 0)
        self.min_retweets = 1
        self.last_update = None
        self.track = None
        self.updating = False

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
                    format_defined='{label}: {rts:.3g} RTs ({rounding} - ' \
                                   '{round_down:.3g}%/{round_up:.3g}%)',
                    format_undefined='{label}: ------'
                ),
            ]
        )

    @property
    def min_retweets(self):
        return int(self.redis.get(self.MIN_RETWEETS_KEY))

    @min_retweets.setter
    def min_retweets(self, value):
        self.redis.set(self.MIN_RETWEETS_KEY, int(value))

    def get_query(self):
        if not self.last_update:
            self.last_update = datetime.datetime.now()
            self.finalize_batch()

            places = [
                1,
                23424975,   # UK
                23424977,   # USA
                23424748,   # Australia
                23424916,   # New Zealand
            ] #+ [place['woeid'] for place in self.api.trends_available()]

            trends = []
            for each in places:
                tts = [
                    topic 
                    for topic in self.api.trends.place(_id=each)[0]['trends']
                    if topic['tweet_volume']
                ]
                trends.extend(tts)

            assert trends, 'rate limited'
            trends = sorted(
                trends, key=lambda t: t['tweet_volume'], reverse=True
            )[:25]
            track = [urllib.unquote(t['query']) for t in trends]

            reef = [
                "reef", "barrier", "GreatBarrier",
                "BarrierReef", "GreatBarrierReef", 
            ]
            track = track #+ reef
            self.updating = False

            print 'refreshed trending topics'
            self.track = track

        print 'will listen to %d trending topics' % len(self.track)
        return ','.join(self.track)

    def run(self, status):
        orig_tweet_id = status.get('retweeted_status', {}).get('id_str', None)

        orig_retweets = 0
        if orig_tweet_id:
            orig_retweets = self.redis.zincrby(self.RETWEETS_SET_KEY, orig_tweet_id, 1)

        min_rts = self.min_retweets
        if orig_retweets >= min_rts:
            self.redis.incr(self.SAVED_COUNT_KEY)
            SimpleStreamListener.run(self, status)
            status = 'SAVED'
        else:
            status = 'SKIPPED'

        self.logger.info('tweet %s was retweeted %d times, threshold is %d, %s',
                         orig_tweet_id, orig_retweets, min_rts, status)
        return status == 'SAVED'

    def on_status(self, status):
        if self.count and self.count % self.STATS_UPDATE_INTERVAL == 0:
            min_rts = self.update_statistics()
            now = datetime.datetime.now()
            since_update = (now - self.last_update).total_seconds()
            if (min_rts >= self.MAX_RETWEET_THRESHOLD or
                    since_update > self.MAX_TIME_ON_TOPIC):
                self.finalize_batch()
                self.last_update = None
                self.track = None
                print 'Retweet threshold exceeded'
                return False

        super(InterestingStuffStreamListener, self).on_status(status)

    def finalize_batch(self):
        print 'flushing tweets'
        rts = self.redis.zrangebyscore(self.RETWEETS_SET_KEY,
                                       self.min_retweets, '+infinity')
        if rts:
            expand_user_of_tweets.delay(rts[:25])
        self.redis.delete(self.RETWEETS_SET_KEY)

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
        dist_up = self.TOP_RETWEET_PERCENT - pcent_rounded_up 
        dist_down = pcent_rounded_down - self.TOP_RETWEET_PERCENT
        if dist_up < dist_down:
            min_rts = scores[int(top_k_pcentile)] + 1
            rounding = 'U'
        else:
            min_rts = scores[int(top_k_pcentile)]
            rounding = 'D'

        assert int(min_rts) > 0  # shouldn't happen, right?
        self.min_retweets = min_rts

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

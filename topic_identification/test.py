from nltk.sentiment.vader import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
import sys
import itertools
from urlparse import urlparse
from collections import defaultdict
from treetaggerwrapper import make_tags, Tag, TreeTagger
from sklearn.feature_extraction.text import TfidfVectorizer
import pytz
import datetime
import math
from dateutil.parser import parse
import random
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

query = {
    'query': {
        'bool': {
            'filter': {
                'bool': {
                    'should': [
#                       {'match': {'text': 'great'}},
#                       {'match': {'text': 'reef'}},
#                       {'match': {'text': 'barrier'}},
#                       {'match': {'text': 'GreatBarrier'}},
#                       {'match': {'text': 'BarrierReef'}},
#                       {'match': {'text': 'GreatBarrierReef'}},
                        {'match': {'text': 'nobel'}},
                        {'match': {'text': 'prize'}},
                    ]
                }
            }
        }
    }
}

def scan(f):
    with open('../dev/tweets-nobel-sample.jsonl') as fi:
        for i, x in enumerate(fi):
            f(i, json.loads(x))


class Counter:
    min_dt = None
    max_dt = None
    count = 0
    
    def __call__(self, i, tweet):
        dt = parse(tweet['_source']['created_at'])
        self.min_dt = min(self.min_dt, dt) if self.min_dt else dt
        self.max_dt = max(self.max_dt, dt) if self.max_dt else dt
        self.count += 1


class Bucketizer:
    # why the fuck an UTC datetime should NOT BE localized
    epoch = pytz.utc.localize(datetime.datetime.utcfromtimestamp(0))

    def __init__(self, min_dt, max_dt, num_buckets=None, bucket_width=None):
        self.min_dt = self.norm(min_dt)
        self.max_dt = self.norm(max_dt)
        if num_buckets > 0:
            self.bucket_width = (self.max_dt - self.min_dt) / num_buckets
        elif bucket_width > 0:
            self.bucket_width = bucket_width
        else:
            raise ValueError('must specify either num_buckets or bucket_width')
        self.buckets = {}

    def __call__(self, i, tweet):
        dt = self.norm(parse(tweet['_source']['created_at']))
        bucket = int((dt - self.min_dt) / self.bucket_width)
        self.buckets[tweet['_source']['id_str']] = bucket

    def norm(self, dt):
        return (dt - self.epoch).total_seconds()


class BucketCounter:
    def __init__(self):
        self.count = 0

    def process(self, tweet):
        self.count += 1

    def __repr__(self):
        return 'count: %d' % self.count


class BucketSentimentAnalyzer:
    def __init__(self):
        self.sid = SentimentIntensityAnalyzer()
        self.agg = {}
        self.count = 0

    def process(self, tweet):
        self.count  += 1
        sent = self.sid.polarity_scores(tweet['_source']['text'])
        for k, v in sent.iteritems():
            if k not in self.agg:
                self.agg[k] = v
            self.agg[k] = max(self.agg[k], v)

    def __repr__(self):
        return ' - '.join('%s: %.2f' % (k, v)
                          for k, v in self.agg.iteritems())


class BucketCoOccurrences:
    def __init__(self):
        language = 'en'
        self.counters = defaultdict(lambda: 0)
        self.tweet_count = 0
        self.tagger = TreeTagger(
            TAGLANG=language,
            TAGOPT=u'-token -lemma -sgml -quiet',
        )

    def pos_tag(self, text):
        return make_tags(self.tagger.tag_text(text))

    def process(self, tweet):
        self.tweet_count += 1
        text = tweet['_source']['text']
        remove = []

        for hashtag in tweet['_source']['entities']['hashtags']:
            self.counters['#' + hashtag['text']] += 1
            remove.append(hashtag['indices'])

        for user in tweet['_source']['entities']['user_mentions']:
            self.counters['@' + user['screen_name']] += 1
            remove.append(user['indices'])

        # FIXME do we want websites?
        """
        for url in tweet['_source']['entities']['urls']:
            if not url['expanded_url']:
                continue
            parsed = urlparse(url['expanded_url'])
            remove.append(url['indices'])
            if parsed.netloc:
                self.counters['http://' + parsed.netloc] += 1
        """

        skipped = 0
        remove = sorted(remove, key=lambda (s, e): s)
        for bounds in remove:
            start, end = sorted(bounds)
            text = text[:start - skipped] + text[end - skipped:]
            skipped += end - start

        if text.startswith('RT'):
            text = text[2:]

        words = set([
            tag.lemma for tag in self.pos_tag(text.lower())
            if (
                isinstance(tag, Tag) and
                tag.pos[0] == 'N' and
                tag.lemma != '<unknown>' and
                len(tag.lemma) > 1
            )
        ])
        for occ in itertools.combinations(words, 2):
            w1, w2 = sorted(occ)
            self.counters[(w1, w2)] += 1
            
    def __repr__(self):
        most_mentioned = sorted(self.counters.items(),
                                key=lambda (k, v): -self.counters[k])[:3]
        return '%d tweets, most mentioned: %s' % (
            self.tweet_count,
            ','.join('%s (%d)' % m for m in most_mentioned)
        )


class BucketMultiplexer:
    def __init__(self, buckets, *stats):
        self.buckets = buckets
        self.bucket_stats = {
            cls: {} for cls in stats
        }

    def __call__(self, i, tweet):
        bucket = self.buckets[tweet['_source']['id_str']]
        for cls, buckets in self.bucket_stats.iteritems():
            if bucket not in buckets:
                buckets[bucket] = cls()
            buckets[bucket].process(tweet)

    def __str__(self):
        lines = []
        for k, v in self.bucket_stats.iteritems():
            lines.append('*** %s ***' % k.__name__)
            for bucket in sorted(v.keys()):
                lines.append(('   ', bucket, v[bucket]))
            lines.append('')
        return '\n'.join(' '.join(map(str, l)) if isinstance(l, (tuple, list)) else l
                         for l in lines)

    def dump(self):
        return {
            cls.__name__: {
                bucket: stats.dump() for bucket, stats in buckets.iteritems()
            } for cls, buckets in self.bucket_stats.iteritems()
        }


def detect_peaks(values, lag, diff_stdev, influence, diff_min=None):
    mean = stdev = None

    peaks = []
    cur_peak = None
    for i, y in enumerate(values):
        if i == 0:
            mean = y
            stdev = 0
        elif (i < lag or (y < mean + diff_stdev * stdev or
                diff_min <= 0 or abs(y - mean) < diff_min)):

            stdev = (stdev + math.sqrt((y - mean)**2)) / 2
            mean = (mean + y) / 2
            if cur_peak:
                peaks.append(cur_peak)
                cur_peak = None
        else:
            stdev = (stdev + influence*math.sqrt((y - mean)**2)) / (1 + influence)
            mean = (mean + influence * y) / (1 + influence)
            if not cur_peak:
                cur_peak = []
            cur_peak.append(i)

    if cur_peak:
        peaks.append(cur_peak)
        cur_peak = None

    return peaks

def test_peaks(data):
    data = data or [1,2,3,1,3,2,2,10,9,3,1,2,1]
    ps = detect_peaks(data, 3, 3.0, 0.1)
    plot_peaks(data, ps)

def plot_peaks(data, ps, show=True):
    plt.plot(range(len(data)), data, 'r-')
    for each in ps:
        plt.axvspan(each[0] - 0.5, each[-1] + 0.5, color='gray', alpha=0.25)
    if show:
        plt.show()

print '\n***', 'computing min/max/count'
c = Counter()
scan(c)

print '\n***', 'computing buckets'
br = Bucketizer(c.min_dt, c.max_dt,
                bucket_width=datetime.timedelta(hours=4).total_seconds())
scan(br)

print '\n***', 'computing statistics over buckets'
bm = BucketMultiplexer(br.buckets, BucketCoOccurrences)
scan(bm)


print '\n***', 'computing peaks'
series = defaultdict(lambda: [])
for bucket, stats in bm.bucket_stats[BucketCoOccurrences].iteritems():
    for k, v in stats.counters.iteritems():
        if series[k]:
            for i in xrange(series[k][-1][0] + 1, bucket):
                series[k].append((i, 0))
        series[k].append((bucket, v))

keywords = []
for k, values in series.iteritems():
    lag = max(int(0.05 * len(values)), 3)
    diff_stdev = 2.5
    diff_min = 25
    influence = 0.01

    values = [y for x, y in values]
    if len(values) < 4 or max(values) - min(values) < 50:
        continue

    ps = detect_peaks(values, lag, diff_stdev, influence, diff_min)
    if not ps:
        continue

    keywords.extend([(k, (p[0] - 0.5, p[-1] + 0.5)) for p in ps])
    plot_peaks(values, ps, show=False)
    plt.title(k)
    #plt.show()
    print k


print '\n***', 'merging keywords'
keywords = sorted(keywords, key=lambda (w, (s, e)): s)
merged = []
i = 0
min_overlap = 0.75
while i < len(keywords):
    word, (start, end) = keywords[i]
    j = i + 1
    while j < len(keywords) and keywords[j][1][0] < end:
        _, (a, b) = keywords[j]
        overlap = float(end - a) / min(end - start, b - a)
        if overlap < min_overlap:
            break
        j += 1

    topic = set()
    for words, _ in keywords[i:j]:
        if isinstance(words, (tuple, list)):
            for w in words:
                topic.add(w)
        else:
            topic.add(words)
    merged.append(topic)
    i = j


for x in merged:
    print x

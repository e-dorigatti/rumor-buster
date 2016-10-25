from nltk.sentiment.vader import SentimentIntensityAnalyzer
from treetaggerpoll import TaggerProcessPoll
import sys
import re
import numpy as np
import click
import matplotlib.pyplot as plt
import sys
import itertools
from urlparse import urlparse
from collections import defaultdict
from treetaggerwrapper import make_tags, Tag, NotTag, TreeTagger
from sklearn.feature_extraction.text import TfidfVectorizer
import pytz
import datetime
import math
from dateutil.parser import parse
import random
try:
    import ujson as json
except ImportError:
    import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


class Program:  # such java :(
    src = 'http://localhost:9200/tweets/tweet'
    sample = -0.1
    partitions = -1
    co_occurrences = 2
    bucket_width = 4.0
    peaks_lag = 3
    peaks_diff_stdev = 2.5
    peaks_diff_min = 150
    peaks_influence = 0.01
    out_file = 'peaks.jsonl'
    lemma_to_word = 'lemma-to-word.json'
    jaccard_buckets = 50
    participation_buckets = 50

    # why the fuck an UTC datetime should NOT BE localized
    epoch = pytz.utc.localize(datetime.datetime.utcfromtimestamp(0))

    def bucket_for(self, tweet):
        dt = (parse(tweet['created_at']) - self.epoch).total_seconds()
        bucket = int(dt / self.bucket_width)
        return bucket

    def detect_peaks(self, counts):
        counts = list(counts)
        buckets, values = zip(*sorted(counts, key=lambda (b, _): b))
        if len(values) < 4:
            return

        cur_peak = mean = stdev = None
        for i, y in enumerate(values):
            if i == 0:
                mean = y
                stdev = 0
            elif (i < self.peaks_lag or (y < mean + self.peaks_diff_stdev * stdev or
                    self.peaks_diff_min <= 0 or abs(y - mean) < self.peaks_diff_min)):

                stdev = (stdev + math.sqrt((y - mean)**2)) / 2
                mean = (mean + y) / 2
                if cur_peak:
                    yield [buckets[i] for i in cur_peak], counts
                    cur_peak = None
            else:
                stdev = ((stdev + self.peaks_influence * math.sqrt((y - mean)**2)) /
                        (1 + self.peaks_influence))
                mean = (mean + self.peaks_influence * y) / (1 + self.peaks_influence)
                if not cur_peak:
                    cur_peak = []
                cur_peak.append(i)

        if cur_peak:
            yield [buckets[i] for i in cur_peak], counts

    def compute_co_occurrences(self, partition):
        """
        given a set of tweets, finds frequency of URLs, #hashtags, @usermentions,
        as well as a co-occurrence matrix between tuples of words
        """
        tagger = TreeTagger(
            TAGLANG='en',
            TAGOPT=u'-token -lemma -sgml -quiet',
        )

        for bucket, tweet in partition:
            text = tweet['text']
            remove = []
            counters = defaultdict(int)

            for hashtag in tweet['entities']['hashtags']:
                counters['#' + hashtag['text']] += 1
                remove.append(hashtag['indices'])

            for user in tweet['entities']['user_mentions']:
                counters['@' + user['screen_name']] += 1
                remove.append(user['indices'])

            for url in tweet['entities']['urls']:
                if not url['expanded_url']:
                    continue
                remove.append(url['indices'])
                counters[url['expanded_url']] += 1

            # remove urls, hashtags and user mentions from the text
            skipped = 0
            remove = sorted(remove, key=lambda (s, e): s)
            for bounds in remove:
                start, end = sorted(bounds)
                text = text[:start - skipped] + text[end - skipped:]
                skipped += end - start

            if text.startswith('RT '):
                text = text[3:]

            text = text.lower().strip()
            words = set([
                (tag.lemma, tag.word) for tag in make_tags(tagger.tag_text(text))
                if (
                    hasattr(tag, 'lemma')
                    and tag.pos[0] == 'N'
                    and tag.lemma != '<unknown>'
                    and len(tag.lemma) > 1
                )
            ])

            # word co-occurrences
            counters = defaultdict(int)
            for comb in itertools.combinations(words, self.co_occurrences):
                lemmas = sorted(w[0] for w in comb)
                counters[tuple(lemmas)] += 1

            result = {
                'lemma-to-word': words,
                'counters': counters,
            }

            yield bucket, result


    def invert_index(self, (bucket, counters)):
        return [((keyword, bucket), count) for keyword, count in counters.iteritems()]

    def main(self):
        from pyspark import SparkContext, SparkConf
        import pyspark_elastic

        self.bucket_width = datetime.timedelta(
            hours=self.bucket_width
        ).total_seconds()

        conf = SparkConf().setAppName('Find Co-Occurrences')
        sc = pyspark_elastic.EsSparkContext(conf=conf)

        parsed = urlparse(self.src)
        if parsed.netloc:
            es_host, es_port = parsed.netloc.split(':')
            es_resource = parsed.path
            tweets_rdd = (sc.esRDD(es_resource, nodes=es_host, port=es_port)
                .map(lambda (_, t): json.loads(t))
            )
        else:
            tweets_rdd = (sc.textFile(self.src)
                .map(lambda row: json.loads(row))
                .map(lambda t: t.get('_source', t))
            )

        if self.sample > 0.0:
            tweets_rdd = tweets_rdd.filter(lambda _: random.random() < self.sample)

        if self.partitions > 0:
            tweets_rdd = tweets_rdd.repartition(self.partitions)

        tweets_rdd = (tweets_rdd
            .keyBy(self.bucket_for)
            .mapPartitions(self.compute_co_occurrences)
        ).cache()

        peaks = (tweets_rdd
            .map(lambda (b, r): (b, r['counters']))
            .flatMap(self.invert_index)
            .reduceByKey(lambda c1, c2: c1 + c2)
            .map(lambda ((k, b), c): (k, (b, c)))
            .groupByKey()
            .flatMapValues(self.detect_peaks)
            .groupByKey()
        ).collect()

        lemma_to_word = dict((tweets_rdd
            .flatMap(lambda (_, r): r['lemma-to-word'])
            .groupByKey()
            .mapValues(set)
        ).collect())

        print 'Dumping results'
        with open(self.out_file, 'w') as f:
            for keyword, ps in peaks:
                ps = list(ps)
                json.dump({
                    'keyword': keyword,
                    'counts': sorted(ps[0][1], key=lambda (b, _): b),
                    'peaks': [p[0] for p in ps],
                }, f)
                f.write('\n')

    @staticmethod
    def parse_args():
        prog = Program()
        for arg in sys.argv[1:]:
            name, value = re.split(r':|=', arg)
            name = name.replace('-', '_')
            type_ = type(getattr(prog, name))
            parsed = type_(value) if type_ is not type(None) else value
            setattr(prog, name, parsed)
        return prog


if __name__ == '__main__':
    Program.parse_args().main()

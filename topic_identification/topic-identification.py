import matplotlib.pyplot as plt
from ahclustering import AHClustering
import click
import numpy as np
import itertools
from collections import defaultdict
import json


def plot_peak(peak):
    peak_start = min(s for (w, ((s, e), c)) in peak)
    peak_end = max(e for (w, ((s, e), c)) in peak)
    width = peak_end - peak_start

    peak_start -= width * 0.5
    peak_end += width * 0.5

    for word, ((start, end), counts) in peak:
        data = [(x, y) for x, y in counts if peak_start <= x <= peak_end]
        assert data
        data.sort(key=lambda (x, y): x)
        xs, ys = zip(*data)
        plt.plot(xs, ys, '-', label=repr(word))
    plt.show()


def merge_peaks_by_overlap(keywords, min_overlap, plot=False):
    keywords = sorted(keywords, key=lambda (w, ((s, e), c)): s)
    i = 0
    took = set()
    while i < len(keywords):
        _, ((start, end), _) = keywords[i]

        j = i + 1
        while j < len(keywords) and keywords[j][1][0][0] < end:
            _, ((a, b), _) = keywords[j]
            overlap = float(end - a) / min(end - start + 1, b - a + 1)
            if overlap < min_overlap:
                break
            j += 1

        peak = keywords[i:j]
        if plot and len(peak) > 1:
            plot_peak(peak)

        yield peak
        i = j


def split_topics_by_graph(keywords):
    edges = defaultdict(set)
    for hyedge, _ in keywords:
        for w1, w2 in itertools.combinations(hyedge, 2):
            edges[w1].add(w2)
            edges[w2].add(w1)

    word_topic = {}
    def connected_components(word, cc):
        if word not in word_topic:
            word_topic[word] = cc
            for w in edges[word]:
                connected_components(w, cc)

    for word in edges:
        if word not in word_topic:
            connected_components(word, len(word_topic))
    
    topics = defaultdict(set)
    for word, topic in word_topic.iteritems():
        topics[topic].add(word)

    nwords_topics = []
    for topic in topics.values():
        nwords = set()
        for hyedge, _ in keywords:
            if topic & set(hyedge):
                nwords.add(hyedge)
        nwords_topics.append(nwords)
    return nwords_topics


class PeakClustering(AHClustering):
    def __init__(self, linkage, samples):
        peaks = []
        for keyword, ((start, end), counts) in samples:
            peak = {bucket: count for (bucket, count) in counts if bucket <= start <= end}
            peaks.append((keyword, peak))
        super(PeakClustering, self).__init__(linkage, peaks)

    def sample_similarity(self, (keyword1, peak1), (keyword2, peak2)):
        start1, end1 = min(peak1), max(peak1)
        start2, end2 = min(peak2), max(peak2)

        if not (start1 < start2 < end1) and not (start2 < start1 < end2):
            return 0.0

        counts1, counts2 = [], []
        for x in xrange(min(start1, start2), max(end1, end2) + 1):
            counts1.append(peak1.get(x, 0))
            counts2.append(peak2.get(x, 0))
        
        cc = np.corrcoef(counts1, counts2)[0][1]
        return max(cc, 0.0)


def plot_results(hist, hist_sep, hist_clus, small_words, big_words):
    def plot_hist(hist, style, **kwargs):
        xs, ys = zip(*sorted(hist.items(), key=lambda (c, n): c))
        plt.plot(xs, ys, style, **kwargs)

    plt.subplot(1, 2, 1)
    plt.grid(True, which='both')
    plt.grid(which='major', axis='y', linestyle='solid', color='gray')
    plt.grid(which='major', axis='x', linestyle='solid', color='gray')
    plot_hist(hist, 'r-o', label='Peak Overlap')
    plot_hist(hist_sep, 'b-o', label='Peak Overlap + Connected Components')
    plot_hist(hist_clus, 'g-o', label='Peak Overlap + Shape Clustering')
    plt.legend(loc='upper right')
    plt.xscale('log')
    plt.yscale('log')

    def plot_bars(hist, pos, **kwargs):
        less_5 = sum(v for k, v in hist.iteritems() if k < small_words)
        between_5_10 = sum(v for k, v in hist.iteritems() if small_words <= k <= big_words)
        more_10 = sum(v for k, v in hist.iteritems() if k > big_words)
        print less_5, between_5_10, more_10
        offset = [-3, -1, 1][pos]
        plt.bar([10 + offset, 20 + offset, 30 + offset],
                [less_5, between_5_10, more_10],
                width=2, **kwargs)

    plt.subplot(1, 2, 2)
    plt.grid(which='major', axis='y', linestyle='solid', color='gray')
    plt.grid(which='minor', axis='y', linestyle='dotted', color='gray')
    plot_bars(hist, 0, color='r', log=True)
    plot_bars(hist_sep, 1, color='b', log=True)
    plot_bars(hist_clus, 2, color='g', log=True)
    plt.xticks([10, 20, 30], ['Less than %d words' % small_words,
                              'Between %d and %d words' % (small_words, big_words),
                              'More than %d words' % big_words])
    plt.tight_layout()
    plt.show()


@click.command()
@click.argument('peaks-file', type=click.File('r'))
@click.option('--plot', '-p', is_flag=True,
              help='Plot a comparison of the clustering strategies')
@click.option('--size', '-S', type=click.INT, multiple=True,
              help='Only consider keywords of this size')
@click.option('--connected-components', '-c', is_flag=True,
              help='Include peaks clustered using connected components')
@click.option('--min-similarity', '-s', type=click.FLOAT,
              help='Minimum correlation to cluster peaks by shape similarity')
@click.option('--topic-peaks', '-P', is_flag=True,
              help='Include un-clustered peaks')
@click.option('--filter-word', '-f', multiple=True,
              help='Include only peaks with these keywords')
@click.option('--min-words', '-m', type=click.INT,
              help='Include only peaks with at least this many keywords')
@click.option('--max-words', '-M', type=click.INT,
              help='Include only peaks with at most this many keywords')
@click.option('--queries', '-q', type=click.File('w'),
              help='Where to save the queries')
@click.option('--peak-overlap', '-O', type=click.FLOAT, default=0.9,
              help='Merge peaks overlapping in time by at least this much')
def main(peaks_file, plot, connected_components, shape_clustering, filter_word,
         min_words, max_words, queries, size, topic_peaks, peak_overlap):
    peaks = []
    for row in peaks_file:
        keyword = json.loads(row)
        for peak in keyword['peaks']:
            if not size or len(keyword['keyword']) in size:
                peaks.append((tuple(keyword['keyword']),
                             ((peak[0], peak[-1]),
                             tuple(map(tuple, keyword['counts'])))))

    # histograms for plot
    hist_peaks = defaultdict(int)
    hist_sep = defaultdict(int)
    hist_clus = defaultdict(int)

    for trend in merge_peaks_by_overlap(peaks, peak_overlap):
        topics_by_strategy = []

        if plot or connected_components:
            topics_by_strategy.append((hist_sep, split_topics_by_graph(trend)))

        if plot or shape_clustering:
            if not shape_clustering:
                shape_clustering = 0.7

            assert 0.0 < shape_clustering <= 1.0
            clustered = PeakClustering('complete', trend).find_clusters(shape_clustering)
            topics_by_strategy.append(
                (hist_clus, [set(kw for kw, _ in cluster) for cluster in clustered])
            )

        if plot or topic_peaks:
            topics_by_strategy.append((hist_peaks, [set((kw,)) for kw, _ in trend]))

        for hist, topics in topics_by_strategy:
            if not topics:
                continue

            for topic in topics:
                label = set()
                for kws in topic:
                    label.update(set(kws))
                hist[len(label)] += 1

                # optionally filter the topic
                filter_word = set(filter_word) if filter_word else None
                if filter_word and filter_word & label:
                    continue
                elif min_words and len(label) < min_words:
                    continue
                elif max_words and len(label) > max_words:
                    continue

                # build the elasticsearch query
                should = []
                for kw in topic:
                    if len(kw) > 1:
                        should.append({
                            'bool': {
                                'must': [{'match': {'text': w}} for w in kw]
                            }
                        })
                query = {'query': {'bool': {'filter': {'bool': {'should': should}}}}}
                keywords = reduce(lambda l1, l2: l1 + l2, map(list, topics))

                data = {
                    'label': list(label),
                    'keywords': keywords,
                    'query': query,
                }
                print >> queries, json.dumps(data)

    if plot:
        plot_results(hist_peaks, hist_sep, hist_clus, min_words or 5, max_words or 10)


if __name__ == '__main__':
    main()

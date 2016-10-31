import click
import matplotlib.pyplot as plt
import itertools
from collections import defaultdict
import json


STATS = {}
def print_csv(*columns):
    def deco(func):
        def wrapper(*args, **kwargs):
            print ';'.join(columns)
            schema = None
            for each in func(*args, **kwargs):
                if schema is None:
                    schema = []
                    for value in each:
                        if isinstance(value, (str, unicode)):
                            schema.append('%s')
                        elif isinstance(value, int):
                            schema.append('%d')
                        elif isinstance(value, float):
                            schema.append('%f')
                    schema = ';'.join(schema)

                print schema % each

        STATS[func.__name__] = wrapper
        return wrapper
    return deco


def finalize_plot(title, xlabel, ylabel, logx=False):
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, which='both')
    plt.tight_layout()
    plt.legend(loc='upper right')

    if logx:
        plt.xscale('log')


@print_csv('n', 'count')
def peak_count(peaks):
    counts = defaultdict(int)
    for size, ps in peaks.iteritems():
        counts[size] = len(ps)

    counts = sorted(counts.items(), key=lambda (k, v): k)
    plt.plot([x for x, y in counts], [y for x, y in counts], '-o')
    finalize_plot('Number of Peaks', 'Keyword Size', 'Peaks')
    plt.show()

    return counts


@print_csv('n', 'jaccard', 'share', 'count')
def peak_jaccard(peaks):
    for size, ps in sorted(peaks.items(), key=lambda (c, _): c):
        unrolled = reduce(lambda x, y: x + y,
                          [[set(p['keyword'])] * len(p['peaks']) for p in ps],
                          list())

        hist = defaultdict(int)
        for kw1, kw2 in itertools.combinations_with_replacement(unrolled, 2):
            jsim = float(len(kw1 & kw2)) / len(kw1 | kw2)
            hist[int(jsim * 10)] += 1

        cnt = float(sum(hist.values()))
        xs, ys = [], []
        for jacc, count in sorted(hist.items(), key=lambda (j, c): j):
            xs.append(jacc / 10.0)
            ys.append(count / cnt)
            yield size, xs[-1], ys[-1], count

        plt.plot(xs, ys, '-o', label=size)

    finalize_plot('Similarity of Peak\'s Keywords', 'Jaccard Similarity', 'Share')
    plt.show()


@print_csv('n', 'participation', 'share', 'count')
def word_participation(peaks):
    for size, ps in sorted(peaks.items(), key=lambda (c, _): c):
        words = set()

        counts = defaultdict(int)
        for each in ps:
            for word in each['keyword']:
                counts[word] += 1
                words.add(word)

        hist = defaultdict(int)
        for _, cnt in counts.iteritems():
            hist[cnt] += 1

        xs, ys = [], []
        for part in sorted(hist):
            xs.append(part)
            ys.append(float(hist[part]) / len(words))
            yield size, xs[-1], ys[-1], hist[part]

        plt.plot(xs, ys, '-o', label=size)

    finalize_plot('Word Participation', 'Peaks', 'Share', logx=True)
    plt.yscale('log')
    plt.show()


@print_csv('n', 'width', 'share', 'count')
def peak_width(peaks):
    for n in sorted(peaks):
        hist = defaultdict(int)
        for kw in peaks[n]:
            for peak in kw['peaks']:
                hist[peak[-1] - peak[0] + 1] += 1

        cnt = float(sum(hist.values()))
        xs, ys = [], []
        for width in sorted(hist):
            xs.append(width)
            ys.append(hist[width] / cnt)
            yield n, xs[-1], ys[-1], hist[width]

        plt.plot(xs, ys, '-o', label=n)

    finalize_plot('Peak Width', 'Width (Buckets)', 'Share')
    plt.show()


@print_csv()
def plot_peaks(peaks):
    for n, ps in peaks.iteritems():
        for peak in ps:
            xs = [x for x, y in peak['counts']]
            ys = [y for x, y in peak['counts']]
            plt.plot(xs, ys, 'b-')
    plt.show()
    return []


@click.command()
@click.argument('peaks-file', type=click.File('r'))
@click.option('-s', '--statistic', type=click.Choice(STATS.keys()), multiple=True)
def main(peaks_file, statistic):
    peaks = map(json.loads, peaks_file)
    peaks_by_size = defaultdict(list)
    for each in peaks:
        peaks_by_size[len(each['keyword'])].append(each)

    for each in statistic:
        print '\n*** %s ***' % each.replace('_', ' ')
        STATS[each](peaks_by_size)


if __name__ == '__main__':
    main()

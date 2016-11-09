Topic Identification
===

This process is divided in two steps: first, identify peaks in the usage over time of n-words (in a bag-of-words fashion), then cluster the peaks into meaningful topics.

### Peak detection
The first step is performed using Apache Spark via the `compute_peaks.py` script; for comfort, it can be launched using `compute_peaks.sh`, which runs it in local mode. Individual settings can be specified via the CLI, for example:

```
./compute_peaks.sh partitions=128 nwords_max=6 peaks_diff_min=75 out_file=peaks.jsonl
```

The allowed parameters, and their default values, are:
 - `src = 'http://localhost:9200/tweets/tweet'`: source for the tweets, can be an URL (in which case it is assumed to be an elasticsearch index, with doctype) or a local file.
 - `sample = -0.1`: If between 0 and 1, take this portion of tweets
 - `partitions = -1`: If more than 0, repartition the RDD
 - `bucket_width = 4.0`: Width of the buckets, in hours, to compute the co-occurrences
 - `peaks_diff_stdev = 2.5`: In a peak if the (absolute) difference between the current value and the average is more than `peaks_diff_stdev` times the standard deviation (this condition is AND-ed with `peaks_diff_min`)
 - `peaks_diff_min = 150`: In a peak if the (absolute) difference between the average and the current value is at least this much (this condition is AND-ed with `peaks_diff_stdev`)
 - `peaks_influence = 0.01`: the weight of the samples belonging to peaks, used to update the exponential running average and standard deviation
 - `nwords_max = 6`: the maximum size of n-words to compute for each tweet (n goes from 1 up to this value)
 - `out_file = 'peaks.jsonl'`: where to save the output
 - `include_hashtags = True`: whether to include hashtags in the n-words
 - `include_verbs = False`: whether to include verbs in the n-words (be, do, have are always excluded)
 - `include_adverbs = False`: whether to include adverbs in the n-words
 - `include_adjectives = False`: whether to include adjectives in the n-words

This step performs part-of-speech tagging using [TreeTagger](http://www.cis.uni-muenchen.de/~schmid/tools/TreeTagger/) and its [Python wrapper](https://pypi.python.org/pypi/treetaggerwrapper/2.0.6).

The output format is a [JSON Lines](http://jsonlines.org/) file, in which each line contains an object with three keys:
 - `keyword` is the n-word
 - `counts` is a list whose elements are lists of two items: the bucket number and the number of tweets in that bucket containing that n-word
 - `peaks` is a list of peaks. Each peak is a list of (contiguous) bucket numbers.
 
### Peak Clustering
Clustering of peaks is done via the `topic-identification.py` script, which takes these parameters:

```
Usage: topic_identification.py [OPTIONS] PEAKS_FILE

Options:
  -p, --plot                  Plot a comparison of the clustering strategies
  -S, --size INTEGER          Only consider keywords of this size
  -c, --connected-components  Include peaks clustered using connected
                              components
  -s, --min-similarity FLOAT  Minimum correlation to cluster peaks by shape
                              similarity
  -P, --topic-peaks           Include un-clustered peaks
  -f, --filter-word TEXT      Include only peaks with these keywords
  -m, --min-words INTEGER     Include only peaks with at least this many
                              keywords
  -M, --max-words INTEGER     Include only peaks with at most this many
                              keywords
  -q, --queries FILENAME      Where to save the queries
  -O, --peak-overlap FLOAT    Merge peaks overlapping in time by at least this
                              much
  --help                      Show this message and exit.

```

The output file is, again, in JSON Lines format, in which each line contains an object with these keys:
 - `label`: list of words describing the topic
 - `query`: query that can be used to query elasticsearch to retrieve the relevant tweets (it uses lemmas, so apply the proper mapping to the index)
 - `keywords`: list of n-words that describe the topic

Topics are identified by means of peak clustering. The process is roughly as follows: first, merge peaks that overlap in time by a certain amount (defaults to 90%, note that peaks of the same n-word are considered separately, so every n-word can appear in more than one topic), then cluster the resulting set of peaks according to one of two strategies:
 - Connected Components: Consider each n-word as a set of edges in a graph, connecting the words that appear in it. Then, the connected components in the graph form distinct topics
 - Shape Similarity: Cluster n-words whose have a similar shape, as measured by the correlation coefficient. It is used as a pairwise similarity measure between peaks, and the actual clustering is done through agglomerative hierarchical clustering with complete linkage, stopping at `min_similarity`.

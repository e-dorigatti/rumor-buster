# http://www.laurentluce.com/posts/twitter-sentiment-analysis-using-python-and-nltk/
import functools
import sys
import nltk
from positive import pos_sent
from negative import neg_sent
import click
#from test import test_set


def get_words_in_tweets(tweets):
    all_words = []
    for (words, sentiment) in tweets:
        all_words.extend(words)
    return all_words


def get_word_features(wordlist):
    wordlist = nltk.FreqDist(wordlist)
    # print wordlist.most_common(10)
    word_features = wordlist.keys()
    return word_features


def extract_features(document, word_features):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)
    return features


def classify((word_features, classifier), tweet):
    return classifier.classify(extract_features(tweet.split(), word_features))


@click.command()
@click.argument('output-file', type=click.File('w'))
def main(output_file):
    tweets = []
    for (words, sentiment) in pos_sent + neg_sent:
        words_filtered = [e.lower() for e in words.split() if len(e) >= 3] 
        print words_filtered
        tweets.append((words_filtered, sentiment))

    word_features = get_word_features(get_words_in_tweets(tweets))

    training_set = nltk.classify.apply_features(
        functools.partial(extract_features, word_features),
        tweets
    )
    
    print 'training'
    classifier = nltk.NaiveBayesClassifier.train(training_set)
    #print(nltk.classify.accuracy(classifier, test_set))

    import pickle
    pickle.dump((word_features, classifier), output_file)
    print 'saved'


if __name__ == '__main__':
    main()

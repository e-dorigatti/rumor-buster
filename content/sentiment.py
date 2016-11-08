# http://www.laurentluce.com/posts/twitter-sentiment-analysis-using-python-and-nltk/
import sys
import nltk
from positive import pos_sent
from negative import neg_sent
#from test import test_set

tweets = []



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

word_features = get_word_features(get_words_in_tweets(tweets))

def extract_features(document):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)
    return features

training_set = nltk.classify.apply_features(extract_features, tweets)

def main():
    global tweets
    #tweets = tweet

    
    for (words, sentiment) in pos_sent + neg_sent:
        words_filtered = [e.lower() for e in words.split() if len(e) >= 3] 
        print words_filtered
        tweets.append((words_filtered, sentiment))

    
    global word_features
    word_features = get_word_features(get_words_in_tweets(tweets))

    
    global training_set
    training_set = nltk.classify.apply_features(extract_features, tweets)
    
    print 'training'
    global classifier 
    classifier = nltk.NaiveBayesClassifier.train(training_set)
    #print(nltk.classify.accuracy(classifier, test_set))

    import pickle
    with open('../dev/sentiment-classifier.pkl', 'w') as f:
        pickle.dump(classifier, f)
    print 'saved'
    
def test(tweet):
    return classifier.classify(extract_features(tweet.split()))


if __name__ == '__main__':
    main()

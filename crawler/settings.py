ES_HOST = 'http://localhost:9200'
ES_TWEET_INDEX = 'tweets'
ES_TWEET_INDEX = 'new_tweets'
ES_TWEET_DOCTYPE = 'tweet'
ES_USER_INDEX = 'users'
ES_USER_DOCTYPE = 'user'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

RABBIT_HOST = 'localhost'
RABBIT_PORT =5672
RABBIT_USER = 'guest'
RABBIT_PASS = 'guest'

TWEEPY_CACHE = './tweepydata'

try:
    from settings_prod import *
except ImportError:
    pass

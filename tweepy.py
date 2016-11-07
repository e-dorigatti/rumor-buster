# -*- coding:utf8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import datetime
import time


DB = dict()


with open('data/sample-users.json') as f:
    user_json = json.load(f) 

for user_dict in user_json:
    key = user_dict['_source']['name']
    value = json.dumps(user_dict)
    DB[key] = value


with open('data/sample-user-tweets.json') as f:
    twitters = json.load(f)

for twitter in twitters:
    key = twitter['_id']
    value = json.dumps(twitter)
    DB[key] = value


class OAuthHandler(object):
    def __init__(self, consumer_key, consumer_secret):
        pass

    def set_access_token(self, access_token, access_token_secret):
        pass


class API(object):
    def __init__(self, auth):
        pass

    def get_user(self, screen_name):
        return User(screen_name)

class User(object):
    def __init__(self, user_name):
        self.user_name = user_name
        try:
            self.user_dict = json.loads(DB[user_name])
        except Exception as err:
            print 'cannot find record for %s' % user_name

    def timeline(self):
        twitter_id_lst = self.user_dict['_source']['timeline']
        timeline_lst = []
        for twitter_id in twitter_id_lst:
            try:
                twitter_dict = DB[twitter_id]
                twitter_dict = json.loads(twitter_dict)
            except KeyError:
                continue
            twitter = Twitter(twitter_dict)
            timeline_lst.append(twitter)
            if len(timeline_lst) >= 20:
                break
        return timeline_lst
    

    @property
    def friends_count(self):
        return self.user_dict['_source']['friends_count']
    
    @property
    def followers_count(self):
        return self.user_dict['_source']['followers_count']

    @property
    def created_at(self):
        created_at = self.user_dict['_source']['created_at']
        #created_at = time.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        created_at = time.mktime(time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y'))
        created_at = int(created_at)
        created_at = datetime.datetime.utcfromtimestamp(created_at)
        
        return created_at



class Twitter(object):
    def __init__(self, twitter_dict):
        self.twitter_dict = twitter_dict

    @property
    def text(self):
        return self.twitter_dict['_source']['text']

    @property
    def created_at(self):
        created_at = self.twitter_dict['_source']['created_at']
        created_at = time.mktime(time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y'))
        created_at = int(created_at)
        created_at = datetime.datetime.utcfromtimestamp(created_at)
        return created_at



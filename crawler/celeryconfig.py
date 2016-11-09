import settings

CELERY_RESULT_BACKEND = 'redis://%s:%d/1' % (settings.REDIS_HOST, settings.REDIS_PORT)
BROKER_URL = 'amqp://%s:%s@%s:%d/' % (settings.RABBIT_USER, settings.RABBIT_PASS,
                                      settings.RABBIT_HOST, settings.RABBIT_PORT)

BROKER_HEARTBEAT = True
BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True}
CELERY_ACKS_LATE = False
CELERYD_TASK_SOFT_TIME_LIMIT = 5 * 60
CELERYD_TASK_TIME_LIMIT = 7 * 60

CELERY_ROUTES = {
    'tasks.InterestingStuffStreamListener': {
        'queue': 'realtime'
    },
    'tasks.expand_user_of_tweets': {
        'queue': 'low',
    },
    'tasks.analyze_user': {
        'queue': 'low',
    },
}

CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']


import settings

CELERY_RESULT_BACKEND = 'redis://%s:%d/1' % (settings.REDIS_HOST, settings.REDIS_PORT)
BROKER_URL = 'amqp://%s:%s@%s:%d/' % (settings.RABBIT_USER, settings.RABBIT_PASS,
                                      settings.RABBIT_HOST, settings.RABBIT_PORT)

BROKER_HEARTBEAT = True
BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True}
CELERY_ACKS_LATE = True
CELERYD_TASK_TIME_LIMIT = 60

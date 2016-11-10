Crawler
===

In order to work, the crawler needs a bit of infrastructure, namely RabbitMQ, ElasticSearch, Redis and at one (recommended at leas two) Celery workers. The easiest way to do this is to run the services using docker:

```
docker run -d -p 6379:6379 redis:latest
docker run -d -p 5672:5672 -p 15672:15672 --hostname rumor-rabbit rabbitmq:3-management
docker run -d -p 9200:9200 elasticsearch:latest
```

Note that this configuration does not persist the data in the containers. In order to do that just add [volumes](https://docs.docker.com/engine/tutorials/dockervolumes/) according to your needs: RabbitMQ data is in `/var/lib/rabbitmq/mnesia/rabbit@rumor-rabbit`, ElasticSearch data is in `/usr/share/elasticsearch/data` and Redis data is in `/data`.

The crawler uses three queues: `realtime` is used to process the tweets coming from the stream, `low` contains the users whose timeline has to be retrieved, and `celery` contains all other tasks. Since tasks from the `low` queue take several seconds each, and tasks from the `realtime` queue need to have very low processing time, it is advised to have at least one worker for each of these queues:

```
celery -A tasks worker -l warn --concurrency 2 --autoscale=4,1 -Q realtime -n worker-realtime
celery -A tasks worker -l warn --concurrency 4 -n worker-low -Q low,celery
```

Finally, it is possible to crawl using the "interesting" strategy, which crawls tweets about the 25 most trending topics worldwide and in some English-speaking countries (UK, USA, Australia, New Zealand), as returned by the [trends/place](https://dev.twitter.com/rest/reference/get/trends/place) Twitter API's endpoint.

```
$ python crawl.py interesting
```

The 3 millions tweets crawled during the duration of the project can be downloaded from [https://drive.google.com/open?id=0BwTnXnG7pvmHX3RhNEdvVG1UWFU](https://drive.google.com/open?id=0BwTnXnG7pvmHX3RhNEdvVG1UWFU), while the data for the users from [https://drive.google.com/open?id=0BwTnXnG7pvmHOHBOcHk2SkYzckk](https://drive.google.com/open?id=0BwTnXnG7pvmHOHBOcHk2SkYzckk). Additionally, the `resources` folder contains a sample of tweets related to the Nobel prize and to the Great Barrier Reef.

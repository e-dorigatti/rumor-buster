import click
import logging
from crawler import tasks


@click.group()
def crawl():
    logging.basicConfig()


@crawl.command()
@click.argument('query', nargs=-1)
def tweets(query):
    if not query:
        print 'Either specify tweet ID(s) or a text query'

    for each in query:
        if all(c.isdigit() for c in each):
            tasks.expand_tweet.delay(each, None)
            print 'Crawl scheduled for', each
        else:
            for tweet in tasks.search(each, 99999):
                tasks.expand_tweet.delay(tweet.id, None)
                print 'Crawl scheduled for', tweet.id


@crawl.command()
@click.argument('user', nargs=-1)
def user(user):
    if not user:
        print 'Specify user ID(s) or name(s)'

    for each in user:
        tasks.analyze_user.delay(each)
        print 'Analysis scheduled for', each


@crawl.command()
@crawl.argument('track', nargs=-1)
@click.option('--language', '-l', default=['en'], multiple=True)
def stream(track, language):
    """ listens to the stream and saves live tweets matching the given track """
    tasks.SimpleStreamListener.start(languages=language, track=track)


@crawl.command()
@click.option('--language', '-l', default=['en'], multiple=True)
def trending(language):
    """ adaptively listens to and saves tweets on trending topics """
    tasks.TrendingStreamListener.start(languages=language)


if __name__ == '__main__':
    crawl()

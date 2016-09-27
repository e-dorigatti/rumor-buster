import click
import crawler.tasks


@click.command()
@click.argument('tweet-id', nargs=-1)
def crawl(tweet_id):
    for each in tweet_id:
        crawler.tasks.expand_tweet.delay(each)
    print 'Crawl scheduled'


if __name__ == '__main__':
    crawl()

import click
import crawler.tasks


@click.command()
@click.argument('tweet-id')
def crawl(tweet_id):
    crawler.tasks.expand_tweet.delay(tweet_id)
    print 'Crawl scheduled'


if __name__ == '__main__':
    crawl()

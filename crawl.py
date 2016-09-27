import click
import crawler.tasks


@click.command()
@click.argument('query', nargs=-1)
def crawl(query):
    if not query:
        print 'Either specify tweet ID(s) or a text query'

    for each in query:
        if all(c.isdigit() for c in each):
            crawler.tasks.expand_tweet.delay(each)
            print 'Crawl scheduled for', each
        else:
            for tweet in crawler.tasks.search(each, 99999):
                crawler.tasks.expand_tweet.delay(tweet.id)
                print 'Crawl scheduled for', tweet.id


if __name__ == '__main__':
    crawl()

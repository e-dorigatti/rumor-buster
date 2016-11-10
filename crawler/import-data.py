import click
import json
from crawler import tasks
from progressbar import ProgressBar


@click.command()
@click.argument('in-file', type=click.File('r'))
def main(in_file):
    import_task = tasks.app.tasks['crawler.tasks.SimpleStreamListener']
    for row in ProgressBar()(in_file):
        tweet = json.loads(row)
        import_task.delay(tweet['_source'])


if __name__ == '__main__':
    main()

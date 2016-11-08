import click
import json
from os import path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import multiprocessing as mp
from elasticsearch.exceptions import ConnectionTimeout
import time


def download((label, query, output_dir)):
    es = Elasticsearch()
    fname = path.join(output_dir, 'tweets-%s.jsonl' % '-'.join(label))
    if path.exists(fname):
        print fname, 'already exists, skipping'
        return fname, -1
    
    print 'processing', fname
    tries = 0
    while True:
        try:
            count = 0
            with open(fname, 'w') as f:
                for tweet in scan(es, query=query, index='tweets', doc_type='tweet'):
                    count += 1
                    json.dump(tweet, f)
                    f.write('\n')
                    if count > 10000:
                        print fname, 'too many matches, first 10000 saved'
                        break
            break
        except ConnectionTimeout:
            t = 2**tries
            tries = min(tries + 1, 5)
            print 'connection timeout, sleeping for', t, 'seconds',
            time.sleep(t)

    print count
    return fname, count


@click.command()
@click.argument('topics-file', type=click.File('r'))
@click.option('--output-dir', default='.')
def main(topics_file, output_dir):
    pool = mp.Pool()
    try:
        def get_tasks():
            for row in topics_file:
                data = json.loads(row)
                yield data['label'], data['query'], output_dir

        for fname, count in pool.imap(download, get_tasks()):
            print fname, count
    finally:
        pool.close()
    

if __name__ == '__main__':
    main()

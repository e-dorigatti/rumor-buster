import click
import elasticsearch
import json
from progressbar import ProgressBar
from elasticsearch.helpers import scan


@click.command()
@click.argument('out-file', type=click.File('w'))
@click.option('-i', '--index', default='tweets')
@click.option('-t', '--doc-type', default='tweet')
@click.option('-h', '--host', default='localhost')
@click.option('-p', '--port', default=9200)
def main(out_file, index, doc_type, host, port):
    es = elasticsearch.Elasticsearch(host=host, port=port)
    count = es.count(index=index, doc_type=doc_type)

    with ProgressBar(max_value=count['count']) as bar:
        for i, each in enumerate(scan(es, index=index, doc_type=doc_type)):
            out_file.write(json.dumps(each))
            out_file.write('\n')
            bar.update(i)


if __name__ == '__main__':
    main()

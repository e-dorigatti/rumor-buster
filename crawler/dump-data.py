import click
import elasticsearch
import json
from progressbar import ProgressBar
from elasticsearch.helpers import scan


@click.command()
@click.argument('out-file', type=click.File('w'))
@click.option('-q', '--query')
@click.option('-i', '--index', default='tweets')
@click.option('-t', '--doc-type', default='tweet')
@click.option('-h', '--host', default='localhost')
@click.option('-p', '--port', default=9200)
def main(out_file, query, index, doc_type, host, port):
    if query:
        query = json.loads(query)
    else:
        query = {
            'query': {
                'bool': {
                    'filter': {
                        'bool': {
                            'should': {'match_all': {}}
                        }
                    }
                }
            }
        }

    es = elasticsearch.Elasticsearch(host=host, port=port)
    res = es.search(body=query, index=index, doc_type=doc_type, size=0)

    with ProgressBar(max_value=res['hits']['total']) as bar:
        for i, each in enumerate(scan(es, query=query, index=index, doc_type=doc_type)):
            out_file.write(json.dumps(each))
            out_file.write('\n')
            bar.update(i)


if __name__ == '__main__':
    main()

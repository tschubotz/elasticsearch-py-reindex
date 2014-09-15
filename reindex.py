import argparse
import logging
logging.basicConfig()

from elasticsearch import Elasticsearch

def main(args):
    """Main
    """

    print 'Start reindexing from {0} to {1} with batch size of {2}'.format(args.source_index, args.destination_index, args.batch_size)

    client = Elasticsearch()
    print 'connected to elastic search at http://localhost:9200'

    docs = scan(client, index=args.source_index)

    count = 0

    total = 0

    bulk_items = []

    for doc in docs:
        action, data = create_index_item(doc, args.destination_index)
        bulk_items.append(action)
        bulk_items.append(data)

        action= create_delete_item(doc, args.source_index)
        bulk_items.append(action)

        count += 1
        total += 1

        if count >= args.batch_size:
            client.bulk(body=bulk_items)
            bulk_items = []
            count = 0
            print 'Total done: {0}'.format(total)

    if bulk_items:
        client.bulk(body=bulk_items)
    print 'Total done: {0}'.format(total)


def create_index_item(doc, destination_index):
    """Create a bulk item for indexing doc
    Returns a tuple containing action and data
    """

    action = { 'index' : { '_index' : destination_index, '_type' : doc['_type'] } }
    data = doc['_source']
    return action, data


def create_delete_item(doc, source_index):
    """Create a bulk item for deleting doc
    Returns the action
    """

    action = { 'delete' : { '_index' : source_index, '_type' : doc['_type'], '_id' : doc['_id'] } }
    return action


def scan(client, index, scroll='10m'):
    """Perform scan search and yield hits, one at a time
    """

    response = client.search(search_type='scan', scroll=scroll, index=index)

    print 'Got {0} total hits'.format(response['hits']['total'])

    scroll_id = response['_scroll_id']

    while True:
        response = client.scroll(scroll_id, scroll=scroll)
        if not response['hits']['hits']:
            break
        for hit in response['hits']['hits']:
            yield hit
        scroll_id = response['_scroll_id']


if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Batchwise elasticsearch reindexing for restricted disk space')
        parser.add_argument('-s', '--source_index', required=True, help='Source index', type=str)
        parser.add_argument('-d', '--destination_index', required=True, help='Destination index', type=str)
        parser.add_argument('-b', '--batch_size', required=False, help='#docs per batch', default=1000, type=int)


        main(parser.parse_args())
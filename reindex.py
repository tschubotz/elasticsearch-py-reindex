import argparse
import logging
logging.basicConfig()
import os
import time

from multiprocessing import Pool, Queue

from elasticsearch import Elasticsearch


class Stop(object):
    pass


def main(args):
    """
    Main function
    :param args: argparse dict
    :return: None
    """

    print 'Start reindexing from {0} to {1} with batch size of {2} and {3} worker processes'.format(args.source_index, args.destination_index, args.batch_size, args.processes)

    client = Elasticsearch()
    print 'connected to elastic search at http://localhost:9200'

    docs = scan(client, index=args.source_index)

    count = 0

    queue = Queue(args.batch_size) #don't fill up queue too much
    pool = Pool(args.processes, worker_main, (queue, args.source_index, args.destination_index, args.batch_size,))

    for doc in docs:
        count += 1
        if count % args.batch_size == 0:
            print 'put {0}'.format(count)
        queue.put(doc, True)
    print 'put {0}'.format(count)

    # send stop messages
    for i in range(args.processes):
        queue.put(Stop, True)

    pool.close()
    pool.join()


def worker_main(queue, source_index, destination_index, batch_size):
    """
    Worker main
    :param queue: multiprocessing.Queue
    :param source_index: str
    :param destination_index: str
    :param batch_size: int
    :return: None
    """
    pid = os.getpid()

    client = Elasticsearch()
    print 'process {0} connected to elastic search at http://localhost:9200'.format(pid)

    count = 0
    total = 0
    bulk_items = []

    while True:
        doc = queue.get(True)

        if doc == Stop:
            break

        action, data = create_index_item(doc, destination_index)
        bulk_items.append(action)
        bulk_items.append(data)

        action = create_delete_item(doc, source_index)
        bulk_items.append(action)

        count += 1
        total += 1

        if count >= batch_size:
            client.bulk(body=bulk_items)
            bulk_items = []
            count = 0
            print 'Process {0} total done: {1}'.format(pid, total)

    if bulk_items:
        client.bulk(body=bulk_items)
        print 'Process {0} total done: {1}'.format(pid, total)
    time.sleep(3)

    print 'process {0} finished'.format(pid)


def create_index_item(doc, destination_index):
    """
    Create a bulk item for indexing doc
    Returns a tuple containing action and data
    :param doc: dict (elasticsearch search hit)
    :param destination_index: str
    :return: tuple of action and data for bulk request
    """

    action = { 'index' : { '_index' : destination_index, '_type' : doc['_type'] } }
    data = doc['_source']
    return action, data


def create_delete_item(doc, source_index):
    """
    Create a bulk item for deleting doc
    Returns the action
    :param doc: dict (elasticsearch search hit)
    :param source_index: str
    :return: action dict
    """

    action = { 'delete' : { '_index' : source_index, '_type' : doc['_type'], '_id' : doc['_id'] } }
    return action


def scan(client, index, scroll='10m'):
    """
    Perform scan search and yield hits, one at a time
    :param client: ElasticsearchClient
    :param index: str
    :param scroll: time
    :return: yields search hits
    """

    response = client.search(search_type='scan', scroll=scroll, index=index)

    print 'Got {0} total hits'.format(response['hits']['total'])

    scroll_id = response['_scroll_id']

    while 1:
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
    parser.add_argument('-p', '--processes', required=False, default=1, help='Number of parallel worker processes to use', type=int)

    main(parser.parse_args())
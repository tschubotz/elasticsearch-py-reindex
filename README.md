elasticsearch-py-reindex
========================

Batchwise elasticsearch reindexing for restricted disk space

```
python reindex.py -h
usage: reindex.py [-h] -s SOURCE_INDEX -d DESTINATION_INDEX [-b BATCH_SIZE]
                  [-p PROCESSES]

Batchwise elasticsearch reindexing for restricted disk space

optional arguments:
  -h, --help            show this help message and exit
  -s SOURCE_INDEX, --source_index SOURCE_INDEX
                        Source index
  -d DESTINATION_INDEX, --destination_index DESTINATION_INDEX
                        Destination index
  -b BATCH_SIZE, --batch_size BATCH_SIZE
                        #docs per batch
  -p PROCESSES, --processes PROCESSES
                        Number of parallel worker processes to use
```
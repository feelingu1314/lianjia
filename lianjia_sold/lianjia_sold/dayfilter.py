
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.helpers import scan
from .settings import *

class DayFilter(object):
    def __init__(self):
        self.ignore_count = 0
        self.accept_count = 0

        # mongo parameters
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DB
        self.mongo_crawler_urls = set()

        # es parameters
        self.elasticsearch_node = ELASTICSEARCH_NODE_1
        self.elasticsearch_index = ELASTICSEARCH_INDEX
        self.elasticsearch_type = ELASTICSEARCH_TYPE
        self.es_crawler_urls = set()

        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        for meta in ['sh', 'su']:
            collection = '{}-sold'.format(meta)
            self.mongo_crawler_urls.update(i.get('房源链接') for i in list(db[collection].find({}, {'_id': 0, '房源链接': 1})))

        self.es = Elasticsearch([self.elasticsearch_node])
        results = scan(self.es, query={
            "_source": ["房源链接"],
            "query": {
                "match_all": {
                }
            }
        },
        index=self.elasticsearch_index,
        doc_type=self.elasticsearch_type,
        scroll='1m',
        size=1500
        )

        for result in results:
            self.es_crawler_urls.add(result['_source']['房源链接'])

        print('COUNT IN MONGO =====================================> ', len(self.mongo_crawler_urls))
        print('COUNT IN ELASTICSEARCH =====================================>', len(self.es_crawler_urls))

    def process_item(self):
        if not (self.mongo_crawler_urls - self.es_crawler_urls):
                print('MONGO && ELASTICSEARCH is Syncroniezed !!!')
                return None
        else:
            sync_urls = set()
            sync_urls.update((i, i[8:10]) for i in (self.mongo_crawler_urls - self.es_crawler_urls))
            # sync_urls.update((i, i[8:10]) for i in (self.es_crawler_urls - self.mongo_crawler_urls))
            return sync_urls

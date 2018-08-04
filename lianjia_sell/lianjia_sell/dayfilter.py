from pymongo import MongoClient
from datetime import datetime, timedelta
from .settings import *


class DayFilter(object):
    def __init__(self):
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DB

    def open_connect(self):
        self.client = MongoClient(self.mongo_uri)
        return self.client[self.mongo_db]

    def process_item(self):
        try:
            db = self.open_connect()
            url_today = set()
            url_yesterday = set()
            for meta in ['sh', 'su']:
                collection_today = '{}-sell-{}'.format(meta, (datetime.now()).strftime('%Y-%m-%d'))
                collection_yesterday = '{}-sell-{}'.format(meta, (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'))
                for item in db[collection_today].find({}, {'_id':0, '房源链接':1}):
                    url_today.add(item['房源链接'])
                for item in db[collection_yesterday].find({}, {'_id':0, '房源链接':1}):
                    url_yesterday.add(item['房源链接'])

            self.close_connect()
            if len(url_today) == 0 or len(url_yesterday) == 0 or len(url_yesterday - url_today) <= 0:
                return None
            else:
                return list(url_yesterday - url_today)
        except Exception as e:
            print('ERROR:day_filter =>', e)

    def close_connect(self):
        self.client.close()



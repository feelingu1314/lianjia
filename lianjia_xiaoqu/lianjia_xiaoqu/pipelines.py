# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import re
from datetime import datetime, date
from scrapy.exceptions import DropItem


class LianjiaXiaoquPipeline(object):
    def process_item(self, item, spider):
        item['priceUnit'] = float(item['priceUnit']) / 10000 if re.search(r'\d+', item['priceUnit']) else 0
        item['sale'] = int(item['sale']) if re.search(r'\d+', item['sale']) else 0
        item['deal'] = int(re.search(r'90.*?(\d+)', item['deal'])[1]) if re.search(r'90.*?(\d+)', item['deal']) else 0
        item['rent'] = int(re.search(r'\d+', item['rent'])[0]) if re.search(r'\d+', item['rent']) else 0
        item['age'] = item['age'].split()[-1]
        item['age'] = int(re.search(r'\d+', item['age'])[0]) if re.search(r'\d+', item['age']) else 0
        item['label'] = item['label'] if item['label'] else ''
        item['focus'] = int(re.search(r'\d+', item['focus'])[0]) if re.search(r'\d+', item['focus']) else 0
        item['date'] = date.today().strftime('%Y-%m-%d')
        item['timestamp'] = datetime.now().strftime('%H:%M:%S')
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['codeComm'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item['codeComm'])
        else:
            self.ids_seen.add(item['codeComm'])
            return item


class MongoPipeline(object):

    collection_name = '{}_xiaoqu'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if self.db[self.collection_name.format(item['city'])].count({'date': item['date'], 'code': item['code']}) > 0:
            raise DropItem("duplicate: %s" % item['codeComm'])
        else:
            self.db[self.collection_name.format(item['city'])].insert_one(dict(item))
        return item
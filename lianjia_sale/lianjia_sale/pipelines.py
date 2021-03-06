# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime, date
from scrapy.exceptions import DropItem


class LianjiaSalePipeline(object):
    def process_item(self, item, spider):
        item['priceTotal'] = float(item['priceTotal'])
        item['priceUnit'] = float(item['priceUnit'])/10000 if item['priceUnit'] not in ['暂无数据'] and \
                                                              item['priceUnit'] else 0
        item['area'] = float(item['area'][:-2])
        item['loop'] = item['loop'].strip() if item['loop'] else 'NaN'
        item['community'] = item['community'] if item['community'] else 'NaN'
        item['district'] = item['district'] if item['district'] else 'NaN'
        item['position'] = item['position'] if item['position'] else 'NaN'
        item['codeComm'] = item['codeComm'][0] if item['codeComm'] else 'NaN'
        item['focus'] = int(item['focus'])
        # item['watch'] = int(item['watch'])
        item['dateQuote'] = item['dateQuote'].strip() if item['dateQuote'] not in ['暂无数据'] and item['dateQuote'] else 'NaN'
        item['date'] = date.today().strftime('%Y-%m-%d')
        item['timestamp'] = datetime.now().strftime('%H:%M:%S')
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['code'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item['code'])
        else:
            self.ids_seen.add(item['code'])
            return item


class MongoPipeline(object):

    collection_name = '{}_ershoufang'

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
            raise DropItem("duplicate: %s" % item['code'])
        else:
            self.db[self.collection_name.format(item['city'])].insert_one(dict(item))
        return item
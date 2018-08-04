# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime
# import elasticsearch
from uuid import uuid1
# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk
from scrapy.exceptions import DropItem


class ItemPipeline(object):
    def process_item(self, item, spider):
        for k, v in item.items():
            if k != '小区概况' and isinstance(v, str):
                item[k] = v.strip()
        # Integer
        if item['成交总价'] in ['', 'NA', '暂无数据'] or not item['成交总价']:
            item['成交总价'] = float(0)
        else:
            item['成交总价'] = float(item['成交总价'])
        if item['成交单价'] in ['', 'NA', '暂无数据'] or not item['成交单价']:
            item['成交单价'] = float(0)
        else:
            item['成交单价'] = float(item['成交单价'])
        if item['挂牌价格'] in ['', 'NA', '暂无数据'] or not item['挂牌价格']:
            item['挂牌价格'] = float(0)
        else:
            item['挂牌价格'] = float(item['挂牌价格'])
        if item['浏览'] in ['', 'NA', '暂无数据'] or not item['浏览']:
            item['浏览'] = 0
        else:
            item['浏览'] = int(item['浏览'])
        if item['成交周期'] in ['', 'NA', '暂无数据']:
            item['成交周期'] = 0
        else:
            item['成交周期'] = int(item['成交周期'])
        if item['基本属性']['建成年代'] in ['', 'NA', '暂无数据', '未知']:
            item['基本属性']['建成年代'] = 0
        else:
            item['基本属性']['建成年代'] = int(item['基本属性']['建成年代'])
        if item['基本属性']['建筑面积']:
            item['基本属性']['建筑面积'] = float(item['基本属性']['建筑面积'][:-1])
        else:
            item['基本属性']['建筑面积'] = float(0)
        if item['基本属性']['产权年限'] in ['', 'NA', '暂无数据', '未知']:
            item['基本属性']['产权年限'] = 0
        else:
            item['基本属性']['产权年限'] = int(item['基本属性']['产权年限'][:-1])
        if item['小区概况']['年代'] in ['', 'NA', '暂无数据', '未知']:
            item['小区概况']['年代'] = 0
        else:
            item['小区概况']['年代'] = int(item['小区概况']['年代'][:-1])
        if item['小区概况']['楼栋总数']:
            item['小区概况']['楼栋总数'] = int(item['小区概况']['楼栋总数'][:-1])
        else:
            item['小区概况']['楼栋总数'] = 0
        item['调价次数'] = int(item['调价次数'])
        item['带看'] = int(item['带看'])
        item['关注'] = int(item['关注'])
        # Text
        if not item['小区概况']['小区名']:
            item['小区概况']['小区名'] = '暂无数据'
        # TimeStamp
        item['成交时间'] = item['成交时间'][:10].replace('.', '-')
        item['initial_time'] = datetime.now().date().strftime('%Y-%m-%d')

        return item


class MongoPipeline(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        # name = item.__class__.__name__
        collection = '%s_ershoufang_sold' % item['城市']
        if self.db[collection].count({'房源链接':item['房源链接']}) > 0:
            raise DropItem('Duplicate item found: %s' % item['交易属性']['链家编号'])
        else:
            self.db[collection].insert_one(dict(item))
            # print('SUCCESSFUL:', item['交易属性']['链家编号'])
            return item

    def close_spider(self, spider):
        self.client.close()


class ElasticSearchPipeline(object):
    def __init__(self, node, index, type):
        self.elasticsearch_node = node
        self.elasticsearch_index = index
        self.elasticsearch_type = type

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            node = crawler.settings.get('ELASTICSEARCH_NODE_1'),
            index= crawler.settings.get('ELASTICSEARCH_INDEX'),
            type = crawler.settings.get('ELASTICSEARCH_TYPE')
        )

    def open_spider(self, spider):
        self.es = Elasticsearch([self.elasticsearch_node])
        if not self.es.indices.exists(index=self.elasticsearch_index):
            self.es.indices.create(index=self.elasticsearch_index)
        else:
            print('ELASTICSEARCH.INDEX: {0} is exists'.format(self.elasticsearch_index))

    def es_create(self, data):
        doc = {
            '_op_type': 'create',
            '_index': self.elasticsearch_index,
            '_type': self.elasticsearch_type,
            '_id': uuid1(),
            '_source': dict(data)
        }
        yield doc

    def es_pipeline_datetime(self):
        id = 1
        self.es.ingest.put_pipeline(
            id=id,
            body={
                "description": "crawler.lianjia",
                "processors": [
                    {
                        "date": {
                            "field": "initial_time",
                            "target_field": "@timestamp",
                            "formats": ["Y-M-d"],
                            "timezone": "Asia/Shanghai"
                        }
                    }
                ]
            }
        )
        return id
    
    def process_item(self, item, spider):
        # name = item.__class__.__name__
        item['基本属性'].pop('套内面积')
        item['基本属性'].pop('供暖方式')
        item['小区概况'].pop('hid')
        item['小区概况'].pop('rid')
        item['小区概况'].pop('其他户型')
        item['小区概况'].pop('在售链接')
        item['小区概况'].pop('成交链接')
        item['小区概况'].pop('小区详情')
        item['小区概况'].pop('出租链接')
        
        bulk(self.es, self.es_create(item), pipeline=self.es_pipeline_datetime())
        return item

    def close_spider(self, spider):
        pass
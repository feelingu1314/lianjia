# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime, date
# import redis
from uuid import uuid1
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from scrapy.exceptions import DropItem


class SellPipeline(object):
    def process_item(self, item, spider):
        for k, v in item.items():
            if k != '小区概况' and isinstance(v, str):
                item[k] = v.strip()
        # Integer
        if item['总价'] in ['', '未知', 'NA', '暂无数据'] or not item['总价']:
            item['总价'] = float(0)
        else:
            item['总价'] = float(item['总价'])
        if item['单价'] in ['', '未知', 'NA', '暂无数据'] or not item['单价']:
            item['单价'] = float(0)
        else:
            item['单价'] = float(item['单价'])
        if item['最低首付'] in ['', '未知', 'NA', '暂无数据'] or not item['最低首付']:
            item['最低首付'] = float(0)
        else:
            item['最低首付'] = float(item['最低首付'].strip()[2:-1])
        if item['建造时间'][:4] == '未知年建':
            item['建造时间'] = 0
        else:
            item['建造时间'] = int(item['建造时间'][:4])
        if item['基本属性']['建筑面积'] in ['', '未知', 'NA', '暂无数据'] or not item['基本属性']['建筑面积']:
            item['基本属性']['建筑面积'] = float(0)
        else:
            item['基本属性']['建筑面积'] = float(item['基本属性']['建筑面积'][:-1])
        if item['小区概况']['年代'] in ['', 'NA', '未知', '暂无数据'] or not item['小区概况']['年代']:
            item['小区概况']['年代'] = 0
        else:
            item['小区概况']['年代'] = int(item['小区概况']['年代'][:-1])
        if item['小区概况']['楼栋总数'] in ['', 'NA', '未知', '暂无数据'] or not item['小区概况']['楼栋总数']:
            item['小区概况']['楼栋总数'] = 0
        else:
            item['小区概况']['楼栋总数'] = int(item['小区概况']['楼栋总数'][:-1])
        if item['基本属性']['产权年限'] in ['', 'NA', '未知', '暂无数据'] or not item['基本属性']['产权年限']:
            item['基本属性']['产权年限'] = 0
        else:
            item['基本属性']['产权年限'] = int(item['基本属性']['产权年限'][:-1])
        # Text
        if not item['小区概况']['小区名']:
            item['小区概况']['小区名'] = '暂无数据'
        item['环线信息'] = item['环线信息'].replace('\xa0', '')
        if item['户型分间']:
            temp = dict()
            for i in range(0, len(item['户型分间']), 4):
                temp[item['户型分间'][i]] = item['户型分间'][i + 1:i + 4]
            item['户型分间'] = temp
        else:
            item['户型分间'] = {}
        # TimeStamp
        if item['交易属性']['上次交易'] in ['', 'NA', '未知', '暂无数据']:
            item['交易属性']['上次交易'] = '1987-09-09'
        item['initial_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return item


class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        # name = item.__class__.__name__
        collection = '%s_ershoufang_sell' % item['城市']
        collection_data = '%s_ershoufang_sell_data' % item['城市']
        query1 = self.db[collection].count({'链家编号': dict(item)['链家编号']})
        query2 = self.db[collection].find({'链家编号': dict(item)['链家编号']}, {'_id': 0, 'initial_time': 1})

        if not query1 == 0:
            if not list(query2)[0]['initial_time'][:10] == date.today().strftime('%Y-%m-%d'):
                for key in ['总价', '单价', '最低首付']:
                    self.db[collection_data].update_one(
                        {
                            '链家编号': dict(item)['链家编号']
                        },
                        {
                            '$push':
                                {
                                    key:
                                        {
                                            date.today().strftime('%Y-%m-%d'): item[key]
                                        }
                                }
                        }
                    )
                for key in ['关注人数', '七天带看', '三十天带看']:
                    self.db[collection_data].update_one(
                        {
                            '链家编号': dict(item)['链家编号']
                        },
                        {
                            '$push':
                                {
                                    key:
                                        {
                                            date.today().strftime('%Y-%m-%d'): dict(item)['房源热度'][key]
                                        }
                                }
                        }
                    )
                for key in ['总价', '单价', '最低首付', '房源热度']:
                    item.pop(key)
                self.db[collection].update_one({'链家编号': dict(item)['链家编号']}, {'$set': dict(item)}, True)
            else:
                raise DropItem('daily duplicate item: %s' % item['链家编号'])
        else:
            self.db[collection_data].insert_one(
                {
                    '链家编号': dict(item)['链家编号'],
                    '总价': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['总价']
                        }
                    ],
                    '单价': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['单价']
                        }
                    ],
                    '最低首付': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['最低首付']
                        }
                    ],
                    '关注人数': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['房源热度']['关注人数']
                        }
                    ],
                    '七天带看': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['房源热度']['七天带看']
                        }
                    ],
                    '三十天带看': [
                        {
                            date.today().strftime('%Y-%m-%d'): item['房源热度']['三十天带看']
                        }
                    ]
                }
            )
            for key in ['总价', '单价', '最低首付', '房源热度']:
                item.pop(key)
            self.db[collection].update_one({'链家编号': dict(item)['链家编号']}, {'$set': dict(item)}, True)
        # if query1 == 0:
        #     self.db[collection_data].insert_one(
        #         {
        #             '链家编号': dict(item)['链家编号'],
        #             '总价': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['总价']
        #                 }
        #             ],
        #             '单价': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['单价']
        #                 }
        #             ],
        #             '最低首付': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['最低首付']
        #                 }
        #             ],
        #             '关注人数': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['房源热度']['关注人数']
        #                 }
        #             ],
        #             '七天带看': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['房源热度']['七天带看']
        #                 }
        #             ],
        #             '三十天带看': [
        #                 {
        #                     date.today().strftime('%Y-%m-%d'): item['房源热度']['三十天带看']
        #                 }
        #             ]
        #         }
        #     )
        #     for key in ['总价', '单价', '最低首付', '房源热度']:
        #         item.pop(key)
        #     self.db[collection].update_one({'链家编号': dict(item)['链家编号']}, {'$set': dict(item)}, True)
        # elif list(query2)[0]['initial_time'][:10] == date.today().strftime('%Y-%m-%d'):
        #     raise DropItem('daily duplicate item: %s' % item['链家编号'])
        # else:
        #     for key in ['总价', '单价', '最低首付']:
        #         self.db[collection_data].update_one(
        #             {
        #                 '链家编号': dict(item)['链家编号']
        #             },
        #             {
        #                 '$push':
        #                     {
        #                     key:
        #                         {
        #                             date.today().strftime('%Y-%m-%d'): item[key]
        #                         }
        #                     }
        #             }
        #         )
        #     for key in ['关注人数', '七天带看', '三十天带看']:
        #         self.db[collection_data].update_one(
        #             {
        #                 '链家编号': dict(item)['链家编号']
        #             },
        #             {
        #                 '$push':
        #                     {
        #                     key:
        #                         {
        #                             date.today().strftime('%Y-%m-%d'): dict(item)['房源热度'][key]
        #                         }
        #                     }
        #             }
        #         )
        #     for key in ['总价', '单价', '最低首付', '房源热度']:
        #         item.pop(key)
        #     self.db[collection].update_one({'链家编号': dict(item)['链家编号']}, {'$set': dict(item)}, True)
        return item

    def close_spider(self, spider):
        self.client.close()


class ElasticSearchPipeline(object):
    def __init__(self, node, index, type, mongo_uri, mongo_db):
        self.es_node = node
        self.es_index = index
        self.es_type = type
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        # self.redis_pool = redis.ConnectionPool.from_url(redis_uri)
        # self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            node=crawler.settings.get('ES_NODE'),
            index=crawler.settings.get('ES_INDEX'),
            type=crawler.settings.get('ES_TYPE'),
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            # redis_uri=crawler.settings.get('REDIS_URI')
        )

    def open_spider(self, spider):
        self.es = Elasticsearch([self.es_node])
        if not self.es.indices.exists(index=self.es_index):
            self.es.indices.create(index=self.es_index)

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def es_create(self, data):
        doc = {
            '_op_type': 'create',
            '_index': self.es_index,
            '_type': self.es_type,
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
                            "formats": ["Y-M-d H:m:s"],
                            "timezone": "Asia/Shanghai"
                        }
                    }
                ]
            }
        )
        return id

    def process_item(self, item, spider):
        # name = item.__class__.__name__

        # 如果是别墅，增加'建筑类型','户型结构'的空集合，并且pop'别墅类型'
        if item['交易属性'].pop('房屋用途') == '别墅':
            item['基本属性']['户型结构'] = {}
            item['基本属性']['建筑类型'] = {}
            item['基本属性'].pop('别墅类型')
        item['交易属性'].pop('交易权属')
        item['交易属性'].pop('房屋年限')
        item['交易属性'].pop('抵押信息')
        item['交易属性'].pop('房本备件')
        item['交易属性'].pop('产权所属')
        item['基本属性'].pop('所在楼层')
        item['基本属性'].pop('套内面积')
        item['基本属性'].pop('产权年限')
        item['基本属性'].pop('户型结构')
        item['基本属性'].pop('建筑结构')
        item['基本属性'].pop('建筑类型')
        item['小区概况'].pop('hid')
        item['小区概况'].pop('rid')
        item['小区概况'].pop('小区名')
        item['小区概况'].pop('楼栋总数')
        item['小区概况'].pop('户型总数')
        item['小区概况'].pop('在售')
        item['小区概况'].pop('出租')
        item['小区概况'].pop('类型')
        item['小区概况'].pop('户型推荐')
        item['小区概况'].pop('小区详情')
        # item['小区概况'].pop('经度')
        # item['小区概况'].pop('纬度')
        item.pop('户型分间')
        item.pop('标题')
        item.pop('房源特色')

        collection_data = '%s_ershoufang_sell_data' % item['城市']
        for key in ['总价', '单价', '最低首付', '关注人数', '七天带看', '三十天带看']:
            query = self.db[collection_data].find({'链家编号': dict(item)['链家编号']}, {'_id': 0, key: 1})
            for i in list(query)[0][key]:
                for k, v in i.items():
                    if k == date.today().strftime('%Y-%m-%d'):
                        item.update({key: v})

        bulk(self.es, self.es_create(item), pipeline=self.es_pipeline_datetime())
        return item

    def close_spider(self, spider):
        pass
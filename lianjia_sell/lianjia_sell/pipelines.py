# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime
import redis
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
        item['initial_time'] = datetime.now().date().strftime('%Y-%m-%d')

        return item


class DBPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_collection, redis_uri, redis_key_link):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.redis_uri = redis_uri
        self.redis_key_link = redis_key_link

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
            redis_uri=crawler.settings.get('REDIS_URI'),
            redis_key_link = crawler.settings.get('REDIS_KEY_LINK')
        )

    def open_spider(self, spider):
        self.redis_pool = redis.ConnectionPool.from_url(self.redis_uri)
        self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)
        self.mongo_client = pymongo.MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client[self.mongo_db]

    def process_item(self, item, spider):
        # name = item.__class__.__name__
        name = item['initial_time']
        redis_key_id = item['链家编号']
        redis_key_link = item['房源链接']
        redis_key_totalPrice = 'LianjiaSell:{}:totalPrice'.format(redis_key_id)
        redis_key_unitPrice = 'LianjiaSell:{}:unitPrice'.format(redis_key_id)
        redis_key_mixPayment = 'LianjiaSell:{}:mixPayment'.format(redis_key_id)
        redis_key_follower = 'LianjiaSell:{}:follower'.format(redis_key_id)
        redis_key_day7visitor = 'LianjiaSell:{}:day7visitor'.format(redis_key_id)
        redis_key_day30vistor = 'LianjiaSell:{}:day30visitor'.format(redis_key_id)

        # redis中插入变量数据
        self.redis_client.zadd(redis_key_totalPrice, item['总价'], name)
        self.redis_client.zadd(redis_key_unitPrice, item['单价'], name)
        self.redis_client.zadd(redis_key_mixPayment, item['最低首付'], name)
        self.redis_client.zadd(redis_key_follower, item['房源热度']['关注人数'], name)
        self.redis_client.zadd(redis_key_day7visitor, item['房源热度']['七天带看'], name)
        self.redis_client.zadd(redis_key_day30vistor, item['房源热度']['三十天带看'], name)
        print('SUCCESSFUL:', item['链家编号'])

        # 使用'链家编号'判断是否在redis中存在,存在则不做操作,不存在则插入id并且在mongodb中写入数据
        if not self.redis_client.sismember(self.redis_key_link, redis_key_link):
            self.redis_client.sadd(self.redis_key_link, redis_key_link)
        if self.mongo_db[self.mongo_collection].count({'链家编号':dict(item)['链家编号'], 'initial_time':dict(item)['initial_time']}) > 0:
            raise DropItem('Duplicate item found: %s' % item['链家编号'])
        else:
            # redis数据插入后从item中去除相关变量, 再写入mongodb中
            for key in ['总价', '单价', '最低首付', '房源热度']:
                item.pop(key)
            self.mongo_db[self.mongo_collection].update_one({'链家编号':dict(item)['链家编号']},{'$set':dict(item)},True)
            return item

    def close_spider(self, spider):
        self.mongo_client.close()


class ElasticSearchPipeline(object):
    def __init__(self, node, index, type, redis_uri):
        self.elasticsearch_node = node
        self.elasticsearch_index = index
        self.elasticsearch_type = type
        self.redis_pool = redis.ConnectionPool.from_url(redis_uri)
        self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            node=crawler.settings.get('ELASTICSEARCH_NODE_1'),
            index=crawler.settings.get('ELASTICSEARCH_INDEX'),
            type=crawler.settings.get('ELASTICSEARCH_TYPE'),
            redis_uri = crawler.settings.get('REDIS_URI')
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
        item['小区概况'].pop('经度')
        item['小区概况'].pop('纬度')
        item.pop('户型分间')
        item.pop('标题')
        item.pop('房源特色')

        v1 = self.redis_client.zscore('LianjiaSell:{}:unitPrice'.format(item['链家编号']), item['initial_time'])
        v2 = self.redis_client.zscore('LianjiaSell:{}:totalPrice'.format(item['链家编号']), item['initial_time'])
        v3 = self.redis_client.zscore('LianjiaSell:{}:mixPayment'.format(item['链家编号']), item['initial_time'])
        v4 = self.redis_client.zscore('LianjiaSell:{}:day7visitor'.format(item['链家编号']), item['initial_time'])
        v5 = self.redis_client.zscore('LianjiaSell:{}:day30visitor'.format(item['链家编号']), item['initial_time'])
        v6 = self.redis_client.zscore('LianjiaSell:{}:follower'.format(item['链家编号']), item['initial_time'])
        item.update({'总价': v1, '单价': v2, '最低首付': v3, '七天带看': v4, '三十天带看': v5, '关注人数': v6})

        bulk(self.es, self.es_create(item), pipeline=self.es_pipeline_datetime())
        return item

    def close_spider(self, spider):
        pass
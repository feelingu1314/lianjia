# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from datetime import datetime
from pymongo import MongoClient
import re
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk
# from elasticsearch.helpers import scan

import redis


class LianjiaSoldSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FilterDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.
    def __init__(self, redis_uri, redis_key_link, mongo_uri, mongo_db):
        self.redis_uri = redis_uri
        self.redis_pool = redis.ConnectionPool.from_url(self.redis_uri)
        self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)

        # self.mongo_uri = mongo_uri
        # self.mongo_db = mongo_db
        # self.mongo_client = MongoClient(mongo_uri)
        # self.db = self.mongo_client[self.mongo_db]
        # if not self.redis_client.exists(self.redis_key_link): self.redis_client.sadd(self.redis_key_link, '')

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(
            redis_uri=crawler.settings.get('REDIS_URI'),
            redis_key_link=crawler.settings.get('REDIS_KEY_LINK'),
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        redis_set = '%s_ershoufang_sold:link' % request.url[8:10]
        if self.redis_client.sismember(redis_set, request.url):
            raise IgnoreRequest()
        elif re.search(r'/chengjiao/\d{12}.html', request.url):
            self.redis_client.sadd(redis_set, request.url)
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        redis_set = '%s_ershoufang_sold:link' % response.url[8:10]
        if response.status == 200 and re.search(r'/chengjiao/\d{12}.html', response.url):
            self.redis_client.sadd(redis_set, response.url)
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        return None

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


# class IgnoreDuplicateDownloaderMiddleware(object):
#     def __init__(self, es_node, es_index, es_type):
#         self.ignore_count = 0
#         self.accept_count = 0
#
#         # # mongo parameters
#         # self.mongo_uri = mongo_uri
#         # self.mongo_db = mongo_db
#         # self.mongo_crawler_urls = set()
#
#         # es parameters
#         self.elasticsearch_node = es_node
#         self.elasticsearch_index = es_index
#         self.elasticsearch_type = es_type
#         self.es_crawler_urls = set()
#
#         # client = MongoClient(self.mongo_uri)
#         # db = client[self.mongo_db]
#         # for meta in ['sh', 'su']:
#         #     collection = '{}-sold'.format(meta)
#         #     self.mongo_crawler_urls.update(i.get('房源链接') for i in list(db[collection].find({}, {'_id': 0, '房源链接': 1})))
#
#         self.es = Elasticsearch([self.elasticsearch_node])
#         results = scan(self.es, query={
#             "_source": ["房源链接"],
#             "query": {
#                 "match_all": {
#                 }
#             }
#         },
#         index=self.elasticsearch_index,
#         doc_type=self.elasticsearch_type,
#         scroll='1m',
#         size=1500
#         )
#
#         for result in results:
#             self.es_crawler_urls.add(result['_source']['房源链接'])
#
#         # print('COUNT IN MONGO: ', len(self.mongo_crawler_urls))
#         # print('=================================')
#         # print('COUNT IN ELASTICSEARCH: ', len(self.es_crawler_urls))
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             # mongo_uri = crawler.settings.get('MONGO_URI'),
#             # mongo_db = crawler.settings.get('MONGO_DB'),
#             es_node = crawler.settings.get('ELASTICSEARCH_NODE_1'),
#             es_index = crawler.settings.get('ELASTICSEARCH_INDEX'),
#             es_type = crawler.settings.get('ELASTICSEARCH_TYPE')
#         )
#
#
#     def process_request(self, request, spider):
#         # self.logger.debug('ignore dropping...')
#         if request.url in self.es_crawler_urls:
#             self.ignore_count += 1
#             raise IgnoreRequest()
#         else:
#             self.accept_count += 1
#             return None
#
#     def process_response(self, request, response, spider):
#
#         return response
#
#     def process_exception(self, request, exception, spider):
#
#         return None

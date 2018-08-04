# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

import re
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from datetime import datetime
from pymongo import MongoClient
from redis import ConnectionPool, StrictRedis


class LianjiaSellSpiderMiddleware(object):
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


class LianjiaSellDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
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
        return request

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FilterDownloaderMiddleware(object):
    def __init__(self, mongo_uri, mongo_db, mongo_collection, redis_uri):
        self.redis_uri = redis_uri
        self.redis_pool = ConnectionPool.from_url(self.redis_uri)
        self.redis_client = StrictRedis(connection_pool=self.redis_pool)
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[self.mongo_db]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
            redis_uri = crawler.settings.get('REDIS_URI'),
        )

    def process_request(self, request, spider):
        # self.logger.debug('ignore dropping...')
        if self.redis_client.sismember('LianjiaSell:link', request.url):
            id = re.search(r'\d{12}',request.url).group(0)
            date = datetime.now().date().strftime('%Y-%m-%d')
            if self.redis_client.zrank('LianjiaSell:{}:unitPrice'.format(id), date)\
                    and self.db[self.mongo_collection].count({'房源链接':request.url}) > 0:
                raise IgnoreRequest()
        else:
            return None

    def process_response(self, request, response, spider):
        if not response.status == 200 and re.search(r'/ershoufang/\d{12}.html', response.url):
            if self.redis_client.srem('LianjiaSell:link', response.url):
                print('LINK REMOVED.',response.url)
            raise IgnoreRequest()
        else:
            return response

    def process_exception(self, request, exception, spider):

        return None


# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# import random
import re
from scrapy.exceptions import IgnoreRequest
from datetime import datetime
from scrapy import signals
from datetime import datetime, date
from redis import ConnectionPool, StrictRedis


class LianjiaRedisSpiderMiddleware(object):
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


class FilterMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self, redis_uri):
        self.redis_uri = redis_uri
        self.redis_pool = ConnectionPool.from_url(self.redis_uri)
        self.redis_client = StrictRedis(connection_pool=self.redis_pool)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_uri=crawler.settings.get('REDIS_URI'),
        )

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        if list(filter(lambda x: x in request.url, ['sh.lianjia.com', 'su.lianjia.com'])):
            return None
        else:
            raise IgnoreRequest()

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        if response.status == 200:
            return response
        else:
            with open('ErrorCode.txt','a') as f:
                f.write('Timestamp:{} URL:{} ErrorCode:{}\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request.url, response.status))
            raise IgnoreRequest()

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        # print('DownloaderMiddleware Request URL:', request.url)
        return None

    # def spider_opened(self, spider):
    #     spider.logger.info('Spider opened: %s' % spider.name)


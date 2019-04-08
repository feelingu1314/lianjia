# -*- coding: utf-8 -*-
import scrapy
from redis import ConnectionPool, StrictRedis
from lianjia_sale.items import LianjiaSaleItem


class LianjiaDaySpider(scrapy.Spider):
    name = 'lianjia_day'
    allowed_domains = ['www.lianjia.com']
    # start_urls = ['http://www.lianjia.com/']

    def start_requests(self):
        redis_uri = 'redis://:houwei2019@127.0.0.1:6379/1'
        redis_pool = ConnectionPool.from_url(redis_uri)
        redis_client = StrictRedis(connection_pool=redis_pool)
        for redis_url in redis_client.sscan_iter('lianjia_sale_day:link'):
            yield scrapy.Request(url=redis_url.decode('utf-8'), callback=self.parse_detail, dont_filter=True)

    def parse_detail(self, response):
        item = LianjiaSaleItem()
        item['focus'] = response.xpath('//*[@id="favCount"]/text()').get()
        item['link'] = response.url
        item['label'] = response.xpath('/html/body/div[3]/div/div/div[1]/div/text()').get()
        item['priceTotal'] = response.xpath('/html/body/div[5]/div[2]/div[2]/span[1]/text()').get()
        item['priceUnit'] = response.xpath('/html/body/div[5]/div[2]/div[2]/div[1]/div[1]/span/text()').get()
        item['community'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[1]/a[1]/text()').get()
        item['codeComm'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[1]/a[1]/@href').re('\d+')
        item['district'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/a[1]/text()').get()
        item['position'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/a[2]/text()').get()
        item['loop'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/text()[2]').get()
        item['code'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[4]/span[2]/text()').get()
        item['orient'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[2]/div[1]/text()').get()
        item['area'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[3]/div[1]/text()').get()
        item['room'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[1]/div[1]/text()').get()
        item['dateQuote'] = response.xpath('//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[1]/span[2]/text()').get()
        item['city'] = response.url[8:10]
        yield item

# -*- coding: utf-8 -*-
import scrapy
import re
from redis import ConnectionPool, StrictRedis
from lianjia_xiaoqu.items import LianjiaXiaoquItem


class DaySpider(scrapy.Spider):
    name = 'day'
    allowed_domains = ['www.lianjia.com']
    # start_urls = ['http://www.lianjia.com/']

    def start_requests(self):
        redis_uri = 'redis://:houwei2019@127.0.0.1:6379/1'
        redis_pool = ConnectionPool.from_url(redis_uri)
        redis_client = StrictRedis(connection_pool=redis_pool)
        set_name = 'lianjia_xiaoqu_day:link'
        for i in redis_client.sscan_iter(set_name):
            redis_client.srem(set_name, i)
            tmp = i.decode('utf-8').split('+')
            community = tmp[0].decode('utf-8')
            start_url = re.search(r'https://\w+.lianjia.com/xiaoqu/', tmp[1])[0]
            print(start_url+'rs'+community+'/')
            # yield scrapy.Request(url=redis_url.decode('utf-8'), callback=self.parse, dont_filter=True)

    def parse(self, response):
        # parsing by page
        links = response.xpath('/html/body/div[4]/div[1]/ul/li/div[1]/div[1]/a/@href').getall()
        if links:
            for idx, link in enumerate(links, 1):
                item = LianjiaXiaoquItem()
                item['codeComm'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/@data-id' % idx).get()
                item['community'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[1]/a/text()' % idx).get()
                item['priceUnit'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[2]/div[1]/div[1]/span/text()' % idx).get()
                item['sale'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[2]/div[2]/a/span/text()' % idx).get()
                item['deal'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[2]/a[1]/text()' % idx).get()  # i[-3:-1]
                item['rent'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[2]/a[2]/text()' % idx).get()  # i[0:-5]
                item['district'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/a[1]/text()' % idx).get()
                item['position'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/a[2]/text()' % idx).get()
                item['age'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/text()[4]' % idx).get()  # strip(), i[:4] -> int
                item['label'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[5]/span/text()' % idx).get() \
                    if response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[5]/span/text()' % idx).get() else ''
                item['link'] = link
                item['city'] = re.search(r'https://(.*?).lianjia.com', link)[1]
                print(re.search(r'https://(.*?).lianjia.com', link)[1])
                # yield scrapy.Request(url=link, callback=self.parse_detail, meta={'item': item}, dont_filter=True)

        # curPage = response.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div/@page-data').re('"curPage":(.*?)}')
        # urlPage = response.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div/@page-url').get()
        # if (curPage and urlPage) and int(curPage[0]) < 100:
        #     # print('parse_nextPage:', self.start_url+urlPage.replace('{page}', str(int(curPage[0])+1)))
        #     yield scrapy.Request(url=self.start_url+urlPage.replace('{page}', str(int(curPage[0])+1)), callback=self.parse,
        #                          dont_filter=True)

    def parse_detail(self, response):
        item = response.meta['item']
        item['desc'] = response.xpath('/html/body/div[4]/div/div[1]/div/text()').get()
        item['focus'] = response.xpath('/html/body/div[4]/div/div[2]/div/span/text()').get()
        yield item
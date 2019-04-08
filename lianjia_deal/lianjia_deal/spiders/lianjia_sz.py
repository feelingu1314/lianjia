# -*- coding: utf-8 -*-
import scrapy
import re
from lianjia_deal.items import LianjiaDealItem


class LianjiaSzSpider(scrapy.Spider):
    name = 'lianjia_sz'
    allowed_domains = ['sz.lianjia.com']
    start_url = 'https://sz.lianjia.com'
    # start_urls = ['http://sz.lianjia.com/']

    def start_requests(self):
        yield scrapy.Request(url=self.start_url+'/chengjiao/rs/', callback=self.parse, dont_filter=True)
        yield scrapy.Request(url=self.start_url+'/chengjiao/rs/', callback=self.parse_location, meta={'page': 'index'}, dont_filter=True)

    def parse(self, response):
        pageLinks = response.xpath('/html/body/div[5]/div[1]/ul/li/div/div[1]/a/@href').getall()
        if pageLinks:
            for idx, link in enumerate(pageLinks, 1):
                item = LianjiaDealItem()
                item['houseInfo'] = response.xpath('/html/body/div[5]/div[1]/ul/li[%d]/div/div[2]/div[1]/text()' % idx)\
                    .get()
                item['positionInfo'] = response.xpath('/html/body/div[5]/div[1]/ul/li[%d]/div/div[3]/div[1]/text()'
                                                      % idx).get()
                item['link'] = link
                yield scrapy.Request(url=link, callback=self.parse_detail, meta={'item': item}, dont_filter=True)

        curPage = response.xpath('/html/body/div[5]/div[1]/div[5]/div[2]/div/@page-data').re('"curPage":(.*?)}')
        urlPage = response.xpath('/html/body/div[5]/div[1]/div[5]/div[2]/div/@page-url').get()
        if (curPage and urlPage) and int(curPage[0]) < 100:
            yield scrapy.Request(url=self.start_url + urlPage.replace('{page}', str(int(curPage[0]) + 1)),
                                 callback=self.parse, dont_filter=True)

    def parse_location(self, response):
        # parsing by location
        page = response.meta.get('page', 'ignore')
        if page == 'index':
            districtLinks = response.xpath('/html/body/div[3]/div[1]/dl[2]/dd/div/div/a/@href').getall()
            for link in districtLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse_location, dont_filter=True)
        else:
            locLinks = response.xpath('/html/body/div[3]/div[1]/dl[2]/dd/div/div[2]/a/@href').getall()
            for link in locLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse, dont_filter=True)

    def parse_detail(self, response):
        item = response.meta['item']
        info = response.xpath('/html/body/div[4]/div/text()').get().split()
        item['community'] = info[0]
        item['room'] = info[1]
        item['area'] = info[2]
        item['focus'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[4]/label/text()').get()
        item['watch'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[5]/label/text()').get()
        item['visit'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[6]/label/text()').get()
        item['priceQuote'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[1]/label/text()').get()
        item['priceDeal'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[1]/span/i/text()').get()
        item['priceDealUnit'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[1]/b/text()').get()
        item['dateDeal'] = response.xpath('/html/body/div[4]/div/span/text()').get()
        item['dateQuote'] = response.xpath('//*[@id="introduction"]/div[1]/div[2]/div[2]/ul/li[3]/text()').get()
        item['periodDeal'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[2]/label/text()').get()
        item['priceModifyCount'] = response.xpath('/html/body/section[1]/div[2]/div[2]/div[3]/span[3]/label/text()').get()
        item['code'] = response.xpath('/html/body/div[4]/@data-lj_action_resblock_id').get()
        item['codeComm'] = response.xpath('/html/body/div[4]/@data-lj_action_housedel_id').get()
        item['city'] = 'sz'
        yield item

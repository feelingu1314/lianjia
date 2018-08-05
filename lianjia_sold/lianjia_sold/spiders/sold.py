# -*- coding: utf-8 -*-
import scrapy
import json
from lianjia_sold.items import SoldItem


class SoldSpider(scrapy.Spider):
    name = 'sold'
    allowed_domains = ['www.lianjia.com']

    def start_requests(self):
        yield scrapy.Request(url='https://su.lianjia.com/chengjiao/rs/', meta={'city': 'su', 'referer': '1st'},
                             callback=self.parse, dont_filter=True)
        yield scrapy.Request(url='https://sh.lianjia.com/chengjiao/rs/', meta={'city': 'sh', 'referer': '1st'},
                             callback=self.parse, dont_filter=True)

    def parse(self, response):
        meta = response.request.meta['city']
        urls = response.css('div.info > div.title > a::attr(href)').extract()
        print(response.url)
        for url in urls:
            yield scrapy.Request(url=url, meta={'city': meta}, callback=self.parse_one, dont_filter=True)

        if response.request.meta.get('referer', 'other') == '1st':
            districts = response.css(
                'div.position > dl:nth-child(2) > dd > div:nth-child(1) > div > a::attr(href)'
            ).extract()  # 区域查询(list)
            for district in districts:
                url = 'https://{}.lianjia.com{}'.format(meta, district)
                yield scrapy.Request(url=url, meta={'city': meta, 'district': 'location'}, callback=self.parse,
                                     dont_filter=True)

        if response.request.meta.get('district', 'other') == 'location':
            locations = response.css(
                'body > div.m-filter > div.position > dl:nth-child(2) > dd > div > div:nth-child(2) > a::attr(href)'
            ).extract()
            for location in locations:
                url = 'https://{}.lianjia.com{}'.format(meta, location)
                yield scrapy.Request(url=url, meta={'city': meta}, callback=self.parse, dont_filter=True)

        page_url = response.css('div.contentBottom.clear > div.page-box.fr > div::attr(page-url)').extract_first()
        cur_page = response.css('div.contentBottom.clear > div.page-box.fr > div::attr(page-data)').extract_first()
        if page_url and cur_page:
            if json.loads(cur_page)['curPage'] < json.loads(cur_page)['totalPage']:  # totalPage=100
                page_url = page_url.replace('page', '').format(json.loads(cur_page)['curPage'] + 1)
                next_page_url = 'https://{}.lianjia.com{}'.format(meta, page_url)
                yield scrapy.Request(url=next_page_url, meta={'city':meta}, callback=self.parse, dont_filter=True)

    def parse_one(self, response):
        item = SoldItem()
        meta = response.request.meta['city']

        # 成交信息
        title = response.css('body > div.house-title > div::text').extract_first()  # title
        deal_price = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.price > span > i::text'
        ).extract_first()  # 成交总价
        unit_price = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.price > b::text'
        ).extract_first()  # 成交单价
        sale_price = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(1) > label::text'
        ).extract_first()  # 挂牌价格
        deal_time = response.css('body > div.house-title > div > span::text').extract_first()  # 成交时间
        deal_cycle = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(2) > label::text'
        ).extract_first()  # 成交周期
        adjust_price = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(3) > label::text'
        ).extract_first()  # 调价次数
        see = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(4) > label::text'
        ).extract_first()  # 带看次数
        follower = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(5) > label::text'
        ).extract_first()  # 关注人数
        visit = response.css(
            'body > section.wrapper > div.overview > div.info.fr > div.msg > span:nth-child(6) > label::text'
        ).extract_first()  # 浏览次数

        # 基本属性
        item['基本属性'] = {}
        base_attr_keys = response.css('div.base > div.content > ul > li > span::text').extract()  # 基本信息条目
        for i, base_attr_key in enumerate(base_attr_keys, 1):
            base_attr_value = response.css(
                'div.base > div.content > ul > li:nth-child({})::text'.format(i)
            ).extract_first().strip()
            item['基本属性'].update({base_attr_key:base_attr_value})

        # 交易属性
        item['交易属性'] = {}
        trade_attr_keys = response.css('div.transaction > div.content > ul > li > span::text').extract()  # 交易属性条目
        for i, trade_attr_key in enumerate(trade_attr_keys, 1):
            trade_attr_value = response.css(
                'div.transaction > div.content > ul > li:nth-child({})::text'.format(i)
            ).extract_first().strip()
            item['交易属性'].update({trade_attr_key:trade_attr_value})

        # 房源特色
        feature_label_key = response.css(
            '#house_feature > div > div.tags.clear > div.name::text'
        ).extract_first()  # 标签键
        feature_label_value = response.css(
            '#house_feature > div > div.tags.clear > div.content > a::text'
        ).extract()  # 标签值
        if feature_label_value:
            item['房源特色'] = {feature_label_key:feature_label_value}
        else:
            item['房源特色'] = {}

        feature_keys = response.css(
            '#house_feature > div > div.baseattribute.clear > div.name::text').extract()  # 房源特色的条目
        if feature_label_value:
            start = 2  # nth-child的起始值
        else:
            start = 1
        for i, feature_key in enumerate(feature_keys, start):
            feature_value = response.css(
                '#house_feature > div > div:nth-child({}) > div.content::text'.format(i)
            ).extract_first().strip()
            item['房源特色'].update({feature_key:feature_value})

        hid = response.css('body > div.house-title::attr(data-lj_action_resblock_id)').extract_first()  # 1070xxxx
        rid = response.css('body > div.house-title::attr(data-lj_action_housedel_id)').extract_first()  # 5011xxxx

        item['标题'] = title
        item['成交总价'] = deal_price
        item['成交单价'] = unit_price
        item['挂牌价格'] = sale_price
        item['成交时间'] = deal_time
        item['成交周期'] = deal_cycle
        item['调价次数'] = adjust_price
        item['带看'] = see
        item['关注'] = follower
        item['浏览'] = visit
        item['城市'] = meta
        item['房源链接'] = response.url
        item['小区概况'] = {'hid': hid, 'rid': rid}

        if hid and rid:
            community_url = 'https://{0}.lianjia.com/chengjiao/resblock?hid={1}&rid={2}'.format(meta, hid, rid)
            request = scrapy.Request(url=community_url, meta={'city':meta}, callback=self.parse_location,
                                     dont_filter=True)
            request.meta['item'] = item
            yield request

    def parse_location(self, response):
        item = response.meta['item']
        meta = response.request.meta['city']

        jsonresponse = json.loads(response.body_as_unicode())
        name = jsonresponse['data']['resblock']['name']  # 小区名
        avg_price = jsonresponse['data']['resblock']['unitPrice']  # 小区均价
        build_num = jsonresponse['data']['resblock']['buildNum']  # 楼栋数
        frame_num = jsonresponse['data']['resblock']['frameNum']  # 户型数
        frame_url = jsonresponse['data']['resblock']['frameUrl']  # 其他户型=>https://<meta>.lianjia.com/xiaoqu/c5011xxxx/huxing/
        build_type = jsonresponse['data']['resblock']['buildType']  # 建筑类型
        build_age = jsonresponse['data']['resblock']['buildYear']  # 建筑年代
        sell_num = jsonresponse['data']['resblock']['sellNum']  # 在售数
        sell_url = jsonresponse['data']['resblock']['sellUrl']  # 在售链接 => https://<meta>.lianjia.com/ershoufang/c5011xxxx/
        rent_num = jsonresponse['data']['resblock']['rentNum']  # 出租数
        rent_url = jsonresponse['data']['resblock']['rentUrl']  # 出租链接 => https://<meta>.lianjia.com/zufang/c5011xxx/
        view_url = jsonresponse['data']['resblock']['viewUrl']  # 小区详情
        sold_num = jsonresponse['data']['resblockSoldNum']  # 成交数
        sold_url = jsonresponse['data']['resblockSoldUrl']  # 成交链接 => /chengjiao/c5011xxxxxxx/
        if sold_url:
            sold_url = 'https://{}.lianjia.com'.format(meta)+sold_url
            # yield scrapy.Request(url=sold_url, callback=self.parse, dont_filter=True)

        item['小区概况'].update({'小区名': name, '均价': float(avg_price), '年代': build_age, '类型': build_type,
                             '楼栋总数': build_num, '户型数': frame_num, '其他户型':frame_url, '在售': sell_num,
                             '在售链接': sell_url, '出租数': rent_num, '出租链接': rent_url, '成交数': sold_num,
                             '成交链接': sold_url, '小区详情': view_url})

        yield item

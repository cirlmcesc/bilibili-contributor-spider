# -*- coding: utf-8 -*-

import json
import yaml
import math
import scrapy
from scrapy import log
from scrapy.http import Request
from BilibiliContributorSpider.items import VideoItem
from BilibiliContributorSpider.contributors import contributors_id


class ContributorspiderSpider(scrapy.Spider):
    name = 'ContributorSpider'
    allowed_domains = ['bilibili.com']
    start_urls = ['http://www.bilibili.com/']

    def shell_debug(self, response):
        """ debug dom-tree in shell """
        scrapy.shell.inspect_response(response, self)

    def log_debug(self, msg, data, level=log.WARNING):
        """ 错误输出调试 """
        log.msg(u'|```````````````` %s ````````````````|' % msg, level=level)

        if isinstance(data, list):
            for a in data:
                log.msg(a, level=level)
        else:
            log.msg(data, level=level)

        log.msg('|________________ END ________________|', level=level)

    def start_requests(self):
        for cid in contributors_id:
            url = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s" % cid
            yield Request(url, callback=self.parse_contributors_tlist,
                meta={'cid': cid})

    def parse_contributors_tlist(self, response):
        data = json.loads(response.body)
        data = yaml.safe_load(json.dumps(data))
        type_list = data['data']['tlist']

        for tid in type_list:
            total_page = int(math.ceil(float(type_list[tid]['count']) / 30))
            current_page = 1

            while current_page <= total_page:
                url = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&tid=%s&pagesize=30&page=%s&keyword=&order=pubdate" % (
                    response.meta.get('cid'), tid, current_page
                )
                yield Request(url, callback=self.parse_contributors_vlist,
                    meta={'typename': type_list[tid]['name']}
                )
                current_page += 1

    def parse_contributors_vlist(self, response):
        data = json.loads(response.body)
        data = yaml.safe_load(json.dumps(data))
        vedio_list = data['data']['vlist']

        for video in vedio_list:
            videoitem = VideoItem(video)
            videoitem['typename'] = response.meta.get('typename')
            yield videoitem

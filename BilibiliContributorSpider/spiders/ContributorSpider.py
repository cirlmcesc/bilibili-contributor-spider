# -*- coding: utf-8 -*-

import json
import yaml
import math
import scrapy
import time
from scrapy import log
from scrapy.http import Request
from BilibiliContributorSpider.items import VideoItem
from BilibiliContributorSpider.contributors import contributors_id, start_time, end_time
from BilibiliContributorSpider.pipelines import Video


class ContributorspiderSpider(scrapy.Spider):
    name = 'ContributorSpider'
    allowed_domains = ['bilibili.com']
    start_urls = ['http://www.bilibili.com/']
    space_api = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s"
    space_details_api = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&tid=%s&pagesize=30&page=%s&keyword=&order=pubdate"
    av_details_api = "https://api.bilibili.com/x/web-interface/archive/stat?aid=%s"

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
            yield Request(self.space_api % cid, callback=self.parse_contributors_tlist,
                meta={'cid': cid})

    def parse_contributors_tlist(self, response):
        data = json.loads(response.body)
        data = yaml.safe_load(json.dumps(data))
        type_list = data['data']['tlist']

        for tid in type_list:
            total_page = int(math.ceil(float(type_list[tid]['count']) / 30))
            current_page = 1

            while current_page <= total_page:
                url = self.space_details_api % (
                    response.meta.get('cid'), tid, current_page)
                yield Request(url, callback=self.parse_contributors_vlist,
                    meta={'typename': type_list[tid]['name']})
                current_page += 1

    def parse_contributors_vlist(self, response):
        data = json.loads(response.body)
        data = yaml.safe_load(json.dumps(data))
        start_timestamp = int(time.mktime(
            time.strptime(start_time, '%Y-%m-%d %H:%M:%S')))
        end_timestamp = int(time.mktime(
            time.strptime(end_time, '%Y-%m-%d %H:%M:%S')))

        def BetweenTimestamp(timestamp):
            return timestamp >= start_timestamp and timestamp < end_timestamp

        for video in data['data']['vlist']:
            video['typename'] = response.meta.get('typename')

            if BetweenTimestamp(video['created']):
                yield Request(self.av_details_api % video['aid'], callback=self.parse_video_details,
                    meta={"video": video})
                time.sleep(10)

    def parse_video_details(self, response):
        data = json.loads(response.body)
        data = yaml.safe_load(json.dumps(data))
        videoitem = VideoItem(response.meta.get('video'))
        videoitem['share'] = data['data']['share']

        yield videoitem

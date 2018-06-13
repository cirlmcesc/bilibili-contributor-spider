# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from BilibiliContributorSpider.items import VideoItem
from BilibiliContributorSpider.database.Pedoo import ORMModel


class Video(ORMModel):
    table_name = 'video_infomation'

class BilibilicontributorspiderPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, VideoItem) and not Video.has('aid', '=', item.get('aid')):
            video = Video(attributes=dict(item))
            video.save()

        return item



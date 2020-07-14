# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
from scrapy import signals
from pydispatch import dispatcher
# from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
# import datetime
from scrapy.mail import MailSender
# import sys

#date = datetime.date.today().strftime("%d%b%Y")
output_dir = './full_output' #+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def item_type(item):
    return type(item).__name__.replace('Item','').lower()  # TeamItem => team

class MultiCSVItemPipeline(object):
    SaveTypes = ['jobcard','muster']
    exporters = {}
    def __init__(self):
        dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        self.files = dict([(name, open(output_dir+'/'+name+'.csv', 'wb')) for name in self.SaveTypes])
        self.exporters = dict([(name, CsvItemExporter(self.files[name])) for name in self.SaveTypes])
        [e.start_exporting() for e in self.exporters.values()]

    def spider_closed(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]

    def process_item(self, item, spider):
        what = item_type(item)
        if what in set(self.SaveTypes):
            self.exporters[what].export_item(item)
        return item

# -*- coding: utf-8 -*-

# Scrapy settings for fba_full_scrapy project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import datetime
import os
import sys

date = datetime.date.today().strftime("%d%b%Y")
output_dir = os.getcwd()+'/full_output_'+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

BOT_NAME = 'fba_full_scrapy'

SPIDER_MODULES = ['fba_full_scrapy.spiders']
NEWSPIDER_MODULE = 'fba_full_scrapy.spiders'
ITEM_PIPELINES = {'fba_full_scrapy.pipelines.MultiCSVItemPipeline':0}
DOWNLOADER_MIDDLEWARES = {'scrapy.contrib.downloadermiddleware.retry.RetryMiddleware':None,'fba_full_scrapy.retry.RetryMiddleware':500,}
RETRY_TIMES = 19
LOG_STDOUT = True
LOG_LEVEL = 'INFO'
LOG_FILE = output_dir+'/log.txt'
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'fba_scrapy (+http://www.yourdomain.com)'

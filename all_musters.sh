#!/bin/sh
cd /home/ec2-user/fba-full-scrape
git checkout master
/usr/local/bin/scrapy crawl all_musters
/usr/bin/python check_progress.py
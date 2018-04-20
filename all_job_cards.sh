#!/bin/sh
cd /home/ec2-user/fba-full-scrape
git checkout separated_scrape
timeout -s SIGINT 710m /usr/local/bin/scrapy crawl all_job_cards
/usr/bin/python check_progress.py
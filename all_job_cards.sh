#!/bin/bash
#!./venv/bin/python3
# cd /home/ec2-user/fba-full-scrape
git checkout master
source ./venv/bin/activate
timeout -s SIGINT 710m ./venv/bin/scrapy crawl all_job_cards
python3 check_progress.py
deactivate

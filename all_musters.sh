#!/bin/bash
#!./venv/bin/python3
# cd /home/ec2-user/fba-full-scrape
git checkout master
source "./venv/bin/activate"
./venv/bin/scrapy crawl all_musters
python3 check_progress.py
deactivate

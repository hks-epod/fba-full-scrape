#!/bin/sh
DATE_STR=$(/bin/date +"%d%b%Y")
DATE_EMAIL=$(/bin/date +"%d %B %Y")
/nfs/home/E/edodge/.local/bin/scrapy crawl all_job_cards
python27 scrapy_export.py $DATE_STR $DATE_EMAIL
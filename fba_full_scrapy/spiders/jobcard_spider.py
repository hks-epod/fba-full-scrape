import scrapy
from fba_full_scrapy.items import JobcardItem
from fba_full_scrapy.items import MusterItem
from scrapy.http.request import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
import csv
import os
import datetime
from unidecode import unidecode
import mechanize
from scrapy import signals
import sys
import urlparse
import pandas as pd
import logging

colors = {'active':['#00CC33','#D39027'],'inactive':['Red','Gray']}
input_dir = './input'
gp_file = 'gp list.csv'
#date = datetime.date.today().strftime("%d%b%Y")
output_dir = './full_output' #+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


class MySpider(CrawlSpider):
    name = "all_job_cards"
    
    def populate_job_card_urls():
        br = mechanize.Browser()
        br.set_handle_robots(False)

        start_urls = []
        with open(input_dir + '/'+ gp_file, 'rU') as f:
            reader = csv.reader(f)
            #For each panchayat in csv, go to job card link
            for row_in in reader:
                panchayat = row_in[5]
                url = 'http://164.100.129.4/netnrega/IndexFrame.aspx?lflag=eng&District_Code='+row_in[1]+'&district_name='+row_in[0]+'&state_name=MADHYA+PRADESH&state_Code=17&block_name='+row_in[2]+'&block_code='+row_in[3]+'&fin_year=2015-2016&check=1&Panchayat_name='+'+'.join(row_in[4].split(' '))+'&Panchayat_Code='+row_in[5]
                br.open(url)
                br.follow_link(text_regex='Job card/Employment Register')
                soup = BeautifulSoup(br.response().read(), 'lxml')
                active_job_cards = []
                i=-1
                # Identify HH's in panchayat with active job cards
                try:
                    for tr in soup.find_all('table')[3].find_all('tr'):
                        i+=1
                        if i>0:
                            color = tr.find_all('td')[2].find('font')['color']
                            if color in colors['active']:
                                active_job_cards.append(tr.find_all('td')[1].text)
                except:
                    sys.exit("Couldn't parse the job card directory table for url {}".format(url))

                #Add active job card links to start url
                for item in active_job_cards:
                    job_card = item
                    url = 'http://164.100.129.4/netnrega/state_html/jcr.aspx?reg_no='+job_card+'&Panchayat_Code='+panchayat+'&fin_year=2016-2017'
                    start_urls.append(url)
                    with open(output_dir+'/job_card_urls.csv', 'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([job_card,url])

        return start_urls



    def get_mr_tracker():

        if os.path.isfile(output_dir+'/encountered_muster_links.csv') and os.path.getsize(output_dir+'/encountered_muster_links.csv') > 0:
            mr_tracker = pd.read_csv(output_dir+'/encountered_muster_links.csv',header=None,names=['job_card', 'url', 'msr_no', 'muster_url', 'work_code'],usecols=['msr_no','work_code'],encoding='utf-8',dtype={'work_code':object,'msr_no':object})
        else:
            mr_tracker = pd.DataFrame({'msr_no':[], 'work_code':[]},dtype=object)
        
        # this is where we'll check for duplicate musters

        return mr_tracker

    def get_unscraped_jobcards():

        if os.path.isfile(output_dir+'/jobcard.csv') and os.path.getsize(output_dir+'/jobcard.csv') > 0:
            jobcards = pd.read_csv(output_dir+'/jobcard.csv',encoding='utf-8',usecols=['job_card_number'],dtype={'job_card_number':object})
            jobcards = jobcards[jobcards['job_card_number']!='job_card_number'] # Headers get appended every time the scraper runs
        else:
            jobcards = pd.DataFrame({'job_card_number':[]},dtype=object)
    
        job_card_urls = pd.read_csv(output_dir+'/job_card_urls_48.csv',header=None,names=['job_card','url']) # get the master list of job card urls to scrape

        jc_df = pd.merge(job_card_urls,jobcards.drop_duplicates(),how='left',left_on='job_card',right_on='job_card_number')
        
        jc_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']] # keep the job cards that haven't been scraped yet, drop duplicate job cards

        return jc_df

    #######################

    # start/restart business goes here
    
    if not os.path.isfile(output_dir+'/job_card_urls.csv'): # this is the first time through the scrape, need to populate the inital list of job card urls
        start_urls = populate_job_card_urls()
        mr_tracker = pd.DataFrame({'work_code':[],'msr_no':[]},dtype=object)

    else: # need to see how far along in the scrape we are
 
        # take card of the muster rolls first. this way we can mark them in the tracker before the job card requests start populating the encountered muster urls
        mr_tracker = get_mr_tracker()

        # Now take care of the job cards
        jc_df = get_unscraped_jobcards()

        # add the unscraped job cards to the queue
        start_urls = []
        jc_list = jc_df.to_dict(orient='records')
        for job_card in jc_list:
            start_urls.append(job_card['url'])



    def parse(self, response):
        soup = BeautifulSoup(response.body_as_unicode(), 'lxml')
        url = response.url
        url = url.replace('%20',' ').strip()
        #Get top-level job card info
        # try:
        par = urlparse.parse_qs(urlparse.urlparse(url).query)
        panchayat = par['Panchayat_Code'][0]
        job_card = par['reg_no'][0]

        job_card_table = soup.find_all('table')[1]
        job_card_rows = job_card_table.find_all('tr')
        top_data = list()
        top_data = [panchayat,unidecode(job_card_rows[2].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[3].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[5].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(soup.find_all('td')[13].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[6].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[7].find_all('td')[1].text.encode('utf-8').decode('utf-8')),'',unidecode(job_card_rows[9].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[10].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[11].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[12].find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(job_card_rows[12].find_all('td')[3].text.encode('utf-8').decode('utf-8'))]
        bottom_data = list()
        person_table = job_card_table.find_all('table')[0]
        j=-1
        for tr in person_table:
            j += 1
            if j>1 and j<len(person_table)-1:
                # Add job card level and individual level data
                bottom_data = [unidecode(tr.find_all('td')[1].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[2].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[3].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[4].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[5].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[6].text.encode('utf-8').decode('utf-8')),unidecode(tr.find_all('td')[7].text.encode('utf-8').decode('utf-8'))]
                item_data = top_data+bottom_data
                item_data = [item.strip() for item in item_data]
                item = JobcardItem()
                item['panchayat_code'] = item_data[0]
                item['job_card_number'] = item_data[1]
                item['head_of_hh_name'] = item_data[2]
                item['father_husband_name'] = item_data[3]
                item['sc_st_category'] = item_data[4]
                item['reg_date'] = item_data[5]
                item['address'] = item_data[6]
                item['village_name'] = item_data[7]
                item['panchayat_name'] = item_data[8]
                item['block_name'] = item_data[9]
                item['district_name'] = item_data[10]
                item['bpl_status'] = item_data[11]
                item['family_id'] = item_data[12]
                item['person_id'] = item_data[13]
                item['applicant_name'] = item_data[14]
                item['applicant_gender'] = item_data[15]
                item['applicant_age'] = item_data[16]
                item['account_no'] = item_data[17]
                item['bank_po_name'] = item_data[18]
                item['aadhar_no'] = item_data[19]
                yield item

        muster_links = [link for link in response.xpath("//@href").extract() if 'musternew.aspx' in link]
        # Get links to all muster rolls that individual has been listed on.
        for link in muster_links:
            par = urlparse.parse_qs(urlparse.urlparse(link).query)
            work_code = par['workcode'][0].encode('utf-8')
            msr_no = par['msrno'][0].encode('utf-8')
            dt_from = par['dtfrm'][0]
            day = int(dt_from[0:2])
            month = int(dt_from[3:5])
            year = int(dt_from[6:])
            dt = datetime.date(year,month,day)

            if not ((self.mr_tracker.msr_no==msr_no) & (self.mr_tracker.work_code==work_code)).any() and dt>=datetime.date(2015,9,1):

                self.mr_tracker = self.mr_tracker.append({'work_code':work_code,'msr_no':msr_no},ignore_index=True)
                muster_url = ('http://164.100.129.6/netnrega'+link[2:]).replace(';','').replace('%3b','').replace('-','%96').replace('%20','+').replace('!','')
                with open(output_dir+'/encountered_muster_links.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([job_card.encode('utf-8'), url.encode('utf-8'), msr_no.encode('utf-8'), muster_url.encode('utf-8'), work_code.encode('utf-8')])
  

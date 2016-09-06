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
gp_file = 'test gp list.csv'
#date = datetime.date.today().strftime("%d%b%Y")
output_dir = './full_output' #+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

muster_headers = [
    u'',
    u'नाम/पंजीकरण संख्या',
    u'Aadhaar No',
    u'जाति',
    u'गांव',
    u'1',
    u'2',
    u'3',
    u'4',
    u'5',
    u'6',
    u'7',
    u'कुल हाजिरी',
    u'प्रतिदन मजदूर (माप के अनुसार )',
    u'देय राशि',
    u'यात्रा और खान पान का व्यय',
    u'\u0914\u095b\u093e\u0930 \u0938\u092e\u094d\u092c\u0902\u0927\u093f\u0924 \u092d\u0941\u0917\u0924\u093e\u0928',
    u'कुल नकद भुगतान',
    u'खाता क्रमांक',
    u'Postoffice/Bank Name',
    u'Postoffice Code/Branch name',
    u'Postoffice address/Branch code',
    u'Wagelist No.',
    u'Status',
    u'A/c Credited Date',
    u'हस्ताक्षर/अगुठे का निशान'
]

class MySpider(CrawlSpider):
    name = "all_job_cards"
    start_urls = []
    
    def populate_job_card_urls():
        br = mechanize.Browser()
        br.set_handle_robots(False)
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

    def get_mr_tracker():
        if os.path.isfile(output_dir+'/muster.csv'):
            musters = pd.read_csv(output_dir+'/muster.csv',encoding='utf-8')
        else:
            musters = pd.DataFrame({'work_code':[],'msr_no':[]})
        
        mr_tracker = musters[['work_code','msr_no']] # this is where we'll check for duplicate musters

        return mr_tracker

    def get_unscraped_musters():

        if os.path.isfile(output_dir+'/muster.csv'):
            musters = pd.read_csv(output_dir+'/muster.csv',encoding='utf-8')
        else:
            musters = pd.DataFrame({'work_code':[],'msr_no':[]})
        
        encountered_muster_links = pd.read_csv(output_dir+'/encountered_muster_links.csv',header=None,names=['job_card', 'url', 'msr_no', 'muster_url', 'work_code'])

        musters['right'] = 1
        
        mr_df = pd.merge(encountered_muster_links,musters[['msr_no','work_code']].drop_duplicates(),how='left',on=['msr_no','work_code'])
        mr_df = mr_df[pd.isnull(mr_df.right)].drop_duplicates(subset=['msr_no','work_code']) # keep the musters that haven't been scraped yet, drop duplicate musters
        
        return mr_df

    def get_unscraped_jobcards():

        if os.path.isfile(output_dir+'/jobcard.csv'):
            jobcards = pd.read_csv(output_dir+'/jobcard.csv',encoding='utf-8')
        else:
            jobcards = pd.DataFrame('job_card_number':[])

        job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url']) # get the master list of job card urls to scrape

        jc_df = pd.merge(job_card_urls,jobcards[['job_card_number']].drop_duplicates(),how='left',left_on='job_card',right_on='job_card_number')
        
        jc_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']] # keep the job cards that haven't been scraped yet, drop duplicate job cards

        return jc_df

    #######################

    # start/restart businuess goes here
    
    if not os.path.isfile(output_dir+'/job_card_urls.csv'): # this is the first time through the scrape, need to populate the inital list of job card urls
        populate_job_card_urls()

    else: # need to see how far along in the scrape we are
 
        # take card of the muster rolls first. this way we can mark them in the tracker before the job card requests start populating the encountered muster urls
        mr_tracker = get_mr_tracker()
        mr_df = get_unscraped_musters()

        # pass the unscraped musters as Requests, mark them in the tracker
        muster_list = mr_df.to_dict(orient='records')
        for muster in muster_list:
            mr_tracker = mr_tracker.append({'work_code':muster['work_code'],'msr_no':muster['msr_no']},ignore_index=True)
            yield Request(muster['muster_url'], callback=self.handle_muster, priority=1)

        # Now take care of the job cards
        jc_df = get_unscraped_jobcards()

        # add the unscraped job cards to the queue
        jc_list = jc_df.to_dict(orient='records')
        for job_card in jc_list:
            start_urls.append(job_card['url'])


    def handle_muster(self, response):
        soup = BeautifulSoup(response.body_as_unicode(), 'lxml')
        url = response.url
        url = url.replace('%20',' ').strip()
        item_data = []
        # logging.info("Made it to the muster page for "+url) 
        if soup.find_all('table')[2].find('b').text!='The Values specified are wrong, Please enter Proper values' and soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"})!=None:
    panchayat = url.split('panchayat_code=')[1].split('&msrno')[0]
    mrTopData = [
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"}).text.encode('utf-8').strip().decode('utf-8')),
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldatefrom"}).text.encode('utf-8').strip().decode('utf-8')),
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldateto"}).text.encode('utf-8').strip().decode('utf-8')),
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblSanctionDate"}).text.encode('utf-8').strip().decode('utf-8')),
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkCode"}).text.encode('utf-8').strip().decode('utf-8')),
        unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkName"}).text.encode('utf-8').strip().decode('utf-8'))
    ]

    # If link doesnt work (ie table doesn't show up), put empty rows/cols
    if soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'})==None:
        item_data = [panchayat,'','','','','','','','','','','','','','','','','','','','']+mrTopData
        item = MusterItem()
        item['panchayat_code'] = item_data[0]
        item['job_card_number'] = item_data[1]
        item['worker_name'] = item_data[2]
        item['aadhar_no'] = item_data[3]
        item['sc_st_category'] = item_data[4]
        item['village_name'] = item_data[5]
        item['present'] = item_data[6]
        item['days_worked'] = item_data[7]
        item['average_daily_wage'] = item_data[8]
        item['dues'] = item_data[9]
        item['travel_food_expenses'] = item_data[10]
        item['tool_payments'] = item_data[11]
        item['total_cash_payments'] = item_data[12]
        item['account_no'] = item_data[13]
        item['bank_po_name'] = item_data[14]
        item['po_code_branch_name'] = item_data[15]
        item['po_address_branch_code'] = item_data[16]
        item['wagelist_no'] = item_data[17]
        item['status'] = item_data[18]
        item['signature'] = item_data[19]
        item['ac_credited_date'] = item_data[20]
        item['msr_no'] = item_data[21]
        item['work_start_date'] = item_data[22]
        item['work_end_date'] = item_data[23]
        item['work_approval_date'] = item_data[24]
        item['work_code'] = item_data[25]
        item['work_name'] = item_data[26]
        yield item
    else:

        # Check the number of muster column headers
        if len(soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[0].find_all('th'))!=26:
            logging.info("Found the wrong number of muster column headers for url: " + url)

        # Check if the column headers have changed
        headers_correct = True
        for i,th in enumerate(soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[0].find_all('th')):
            test = th.text.strip()==muster_headers[i].strip()
            if test==False:
                headers_correct = False
        if headers_correct==False:
            logging.info("Incorrect muster column headers found for url: "+url)

        days = list() # Need to get which columns contain the days worked -- if the header is an integer, it's one of the days worked columns
        for th in soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[0].find_all('th'):
            try:
                int(th.text)
                days.append(th.text)
            except:
                pass
        for tr in soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[1:-1]:
            
            # Get Aadhar number, SC/ST status, Village name
            temp_list = [
                unidecode(val.text.encode('utf-8').strip().decode('utf-8')) for val in tr.find_all('td')[2:5]
            ]

            # Get the columns showing which days they were present and consolidate it into a single field            
            temp_list += [
                ';'.join([str(p+1) if val.text.strip()=='P' else '' for p,val in enumerate(tr.find_all('td')[5:5+len(days)])])
            ]

            # Get the rest of the columns in the row            
            temp_list += [
                unidecode(val.text.encode('utf-8').strip().decode('utf-8')) for val in tr.find_all('td')[5+len(days):]
            ]

            # Add the panchayat code, worker name, job card
            item_data = [
                panchayat, # Panchayat code
                unidecode(tr.find_all('td')[1].find('a').text.encode('utf-8').strip().decode('utf-8')), # Worker name
                unidecode(tr.find_all('td')[1].find('a').previous_sibling.previous_sibling.encode('utf-8').strip().decode('utf-8')) # Job card number
            ] + temp_list + mrTopData

            item = MusterItem()
            item['panchayat_code'] = item_data[0]
            item['job_card_number'] = item_data[1]
            item['worker_name'] = item_data[2]
            item['aadhar_no'] = item_data[3]
            item['sc_st_category'] = item_data[4]
            item['village_name'] = item_data[5]
            item['present'] = item_data[6]
            item['days_worked'] = item_data[7]
            item['average_daily_wage'] = item_data[8]
            item['dues'] = item_data[9]
            item['travel_food_expenses'] = item_data[10]
            item['tool_payments'] = item_data[11]
            item['total_cash_payments'] = item_data[12]
            item['account_no'] = item_data[13]
            item['bank_po_name'] = item_data[14]
            item['po_code_branch_name'] = item_data[15]
            item['po_address_branch_code'] = item_data[16]
            item['wagelist_no'] = item_data[17]
            item['status'] = item_data[18]
            item['ac_credited_date'] = item_data[19]
            item['signature'] = item_data[20]
            item['msr_no'] = item_data[21]
            item['work_start_date'] = item_data[22]
            item['work_end_date'] = item_data[23]
            item['work_approval_date'] = item_data[24]
            item['work_code'] = item_data[25]
            item['work_name'] = item_data[26]

            yield item

        else:
            with open(output_dir+'/bad_mr_links.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow([url])

    def parse(self, response):
        soup = BeautifulSoup(response.body_as_unicode(), 'lxml')
        url = response.url
        url = url.replace('%20',' ').strip()
        #Get top-level job card info
        try:
            panchayat = url.split('Panchayat_Code=')[1].split('&')[0]
            job_card = url.split('reg_no=')[1].split('&')[0]

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
                work_code = par['workcode'][0]
                msr_no = par['msrno'][0]
                dt_from = par['dtfrm'][0]
                day = int(dt_from[0:2])
                month = int(dt_from[3:5])
                year = int(dt_from[6:])
                dt = datetime.datetime(year,month,day)


                if not ((mr_tracker.msr_no==msr_no) & (mr_tracker.work_code==work_code)).any() and dt>=datetime.datetime(2015,9,1): # If we don't find the msr_no/work_code in the scraped data and the date is since 9/1/2016 we want to crawl it
                    mr_tracker = mr_tracker.append({'work_code':work_code,'msr_no':msr_no},ignore_index=True)
                    muster_url = ('http://164.100.129.6/netnrega'+link[2:]).replace(';','').replace('%3b','').replace('-','%96').replace('%20','+').replace('!','')
                    with open(output_dir+'/encountered_muster_links.csv', 'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([job_card.encode('utf-8'), url.encode('utf-8'), msr_no.encode('utf-8'), muster_url.encode('utf-8'), work_code.encode('utf-8')])
                    
                    yield Request(muster_url, callback=self.handle_muster, priority=1)
        
        except:
            logging.info("Couldn't parse the job card response for "+url)    

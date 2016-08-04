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

muster_roll_codes = []
colors = {'active':['#00CC33','#D39027'],'inactive':['Red','Gray']}
input_dir = './input'
#date = datetime.date.today().strftime("%d%b%Y")
output_dir = './full_output' #+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

class MySpider(CrawlSpider):
    name = "all_job_cards"
    start_urls = []
    
    if os.path.isfile(output_dir+'/jobcard.csv'): # need to find the starting point based on already scraped jobcards
        
        # have we scraped all the job cards?
        # have we scraped all the musters?

        jobcards = pd.read_csv(output_dir+'/jobcard.csv')
        musters = pd.read_csv(output_dir+'/muster.csv')

        muster_list = musters.to_dict(orient='records') # populate mr codes with already scraped ones
        for muster in muster_list:
            muster_roll_codes.append([muster['work_code'],muster['mr_no']])

        job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url'])

        jc_df = pd.merge(job_card_urls,jobcards[['job_card_number']].drop_duplicates(),how='left',on_left='job_card',on_right='job_card_number')
        jc_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']]

        if len(jc_df.index)==0: # All the job cards have been scraped
            # Find all the musters that haven't been scraped
            encountered_muster_links = pd.read_csv(output_dir+'/encountered_muster_links.csv',header=None,names=['job_card', 'url', 'msr_no', 'muster_url'])

            mr_df = pd.merge(encountered_muster_links,musters[['mr_no']].drop_duplicates(),how='left',on_left='msr_no',on_right='mr_no')
            mr_df = mr_df[pd.isnull(mr_df.mr_no)].drop_duplicates(subset=['msr_no']) # keep the musters that haven't been scraped yet, drop duplicate musters
            mr_df = mr_df[['job_card','url']].drop_duplicates() # we might end up with unique muster list but non-unique jc list
            for job_card in mr_df.to_dict(orient='records'):
                start_urls.append(job_card['url'])
        else: # need to keep working on the job cards
            for job_card in jc_df.to_dict(orient='records'):
                start_urls.append(job_card['url'])


    else: # populate job card links from job card directory page
        gp_file = input_dir+'/test gp list.csv'
        br = mechanize.Browser()
        br.set_handle_robots(False)
        with open(gp_file, 'rU') as f:
            reader = csv.reader(f)
            #For each panchayat in csv, go to job card link
            for row_in in reader:
                panchayat = row_in[5]
                url = 'http://164.100.129.4/netnrega/IndexFrame.aspx?lflag=eng&District_Code='+row_in[1]+'&district_name='+row_in[0]+'&state_name=MADHYA+PRADESH&state_Code=17&block_name='+row_in[2]+'&block_code='+row_in[3]+'&fin_year=2015-2016&check=1&Panchayat_name='+'+'.join(row_in[4].split(' '))+'&Panchayat_Code='+row_in[5]
                br.open(url)
                br.follow_link(text_regex='Job card/Employment Register')
                soup = BeautifulSoup(br.response().read(), 'html.parser')
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


    def handle_muster(self, response):
        soup = BeautifulSoup(response.body_as_unicode(), 'html.parser')
        url = response.url
        url = url.replace('%20',' ').strip()
        item_data = []
        if soup.find_all('table')[2].find('b').text!='The Values specified are wrong, Please enter Proper values' and soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"})!=None:
            panchayat = url.split('panchayat_code=')[1].split('&msrno')[0]
            mrTopData = [unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldatefrom"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldateto"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblSanctionDate"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkCode"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkName"}).text.encode('utf-8').strip().decode('utf-8'))]
            # If link doesnt work, put empty rows/cols
            if soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'})==None:
                item_data = [panchayat,'','','','','','','','','','','','','','','','','','','','','']+mrTopData
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
                item['payment_date'] = item_data[19]
                item['signature'] = item_data[20]
                item['ac_credited_date'] = item_data[21]
                item['mr_no'] = item_data[22]
                item['work_start_date'] = item_data[23]
                item['work_end_date'] = item_data[24]
                item['work_approval_date'] = item_data[25]
                item['work_code'] = item_data[26]
                item['work_name'] = item_data[27]
                yield item
            else:
                days = list()
                for th in soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[0].find_all('th'):
                    try:
                        int(th.text)
                        days.append(th.text)
                    except:
                        pass
                for tr in soup.find('table', {'id':'ctl00_ContentPlaceHolder1_grdShowRecords'}).find_all('tr')[1:-1]:
                        temp_list = [unidecode(val.text.encode('utf-8').strip().decode('utf-8')) for val in tr.find_all('td')[2:5]]
                        temp_list = temp_list+[';'.join([str(p+1) if val.text.strip()=='P' else '' for p,val in enumerate(tr.find_all('td')[5:5+len(days)])])]
                        temp_list = temp_list+[unidecode(val.text.encode('utf-8').strip().decode('utf-8')) for val in tr.find_all('td')[5+len(days):]]
                        item_data = [panchayat,unidecode(tr.find_all('td')[1].find('a').text.encode('utf-8').strip().decode('utf-8')),unidecode(tr.find_all('td')[1].find('a').previous_sibling.previous_sibling.encode('utf-8').strip().decode('utf-8'))]+temp_list+mrTopData
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
                        item['payment_date'] = item_data[19]
                        item['signature'] = item_data[20]
                        item['ac_credited_date'] = item_data[21]
                        item['mr_no'] = item_data[22]
                        item['work_start_date'] = item_data[23]
                        item['work_end_date'] = item_data[24]
                        item['work_approval_date'] = item_data[25]
                        item['work_code'] = item_data[26]
                        item['work_name'] = item_data[27]
                        yield item
        else:
            with open(output_dir+'/bad_mr_links.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow([url])

    def parse(self, response):
        soup = BeautifulSoup(response.body_as_unicode(), 'html.parser')
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
                # work_code = link.split('workcode=')[1].split('&panchayat_code')[0]
                # msr_no = link.split('msrno=')[1].split('&finyear')[0]
                par = urlparse.parse_qs(urlparse.urlparse(link).query)
                work_code = par['workcode'][0]
                msr_no = par['msrno'][0]
                dt_from = par['dtfrm'][0]
                day = int(dt_from[0:2])
                month = int(dt_from[3:5])
                year = int(dt_from[6:])
                dt = datetime.datetime(year,month,day)
                # with open(output_dir+'/dates.csv', 'a') as f:
                #     writer = csv.writer(f)
                #     writer.writerow([dt_from,day,month,year,dt])


                if [work_code,msr_no] not in muster_roll_codes and dt>=datetime.datetime(2015,9,1):
                    muster_roll_codes.append([work_code,msr_no])
                    muster_url = ('http://164.100.129.6/netnrega'+link[2:]).replace(';','').replace('%3b','').replace('-','%96').replace('%20','+').replace('!','')
                    with open(output_dir+'/encountered_muster_links.csv', 'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([job_card.encode('utf-8'), url.encode('utf-8'), msr_no.encode('utf-8'), muster_url.encode('utf-8')])
                    
                    yield Request(muster_url, callback=self.handle_muster, priority=1)
        
        except:
            logging.info("Couldn't parse the job card response for "+response.url)    

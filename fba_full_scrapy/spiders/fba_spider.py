import scrapy
from fba_full_scrapy.items import JobcardItem
from fba_full_scrapy.items import MusterItem
from scrapy.http.request import Request
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
import csv
import os
import datetime
from unidecode import unidecode
import mechanize
from scrapy import signals
import sys
import urlparse

muster_roll_codes = []
colors = {'active':['#00CC33','#D39027'],'inactive':['Red','Gray']}
input_dir = os.getcwd()+'/input'
date = datetime.date.today().strftime("%d%b%Y")
output_dir = os.getcwd()+'/full_output_'+date
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

class MySpider(CrawlSpider):
    name = "all_job_cards"
    start_urls = []
    gp_file = input_dir+'/gp list.csv'
    br = mechanize.Browser()
    with open(gp_file, 'rU') as f:
        reader = csv.reader(f)
        for row_in in reader:
            panchayat = row_in[5]
            url = 'http://164.100.129.4/netnrega/IndexFrame.aspx?lflag=eng&District_Code='+row_in[1]+'&district_name='+row_in[0]+'&state_name=MADHYA+PRADESH&state_Code=17&block_name='+row_in[2]+'&block_code='+row_in[3]+'&fin_year=2015-2016&check=1&Panchayat_name='+'+'.join(row_in[4].split(' '))+'&Panchayat_Code='+row_in[5]
            br.open(url)
            br.follow_link(text_regex='Job card/Employment Register')
            soup = BeautifulSoup(br.response().read())
            active_job_cards = []
            i=-1
            for tr in soup.find_all('table')[3].find_all('tr'):
                i+=1
                if i>0:
                    color = tr.find_all('td')[2].find('font')['color']
                    if color in colors['active']:
                        active_job_cards.append(tr.find_all('td')[1].text)
            for item in active_job_cards:
                job_card = item
                url = 'http://164.100.129.4/netnrega/state_html/jcr.aspx?reg_no='+job_card+'&Panchayat_Code='+panchayat+'&fin_year=2015-2016'
                start_urls.append(url)

    def handle_muster(self, response):
        soup = BeautifulSoup(response.body_as_unicode())
        url = response.url
        url = url.replace('%20',' ').strip()
        item_data = []
        if soup.find_all('table')[2].find('b').text!='The Values specified are wrong, Please enter Proper values' and soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"})!=None:
            panchayat = url.split('panchayat_code=')[1].split('&msrno')[0]
            mrTopData = [unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblMsrNo2"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldatefrom"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lbldateto"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblSanctionDate"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkCode"}).text.encode('utf-8').strip().decode('utf-8')),unidecode(soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblWorkName"}).text.encode('utf-8').strip().decode('utf-8'))]
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
        soup = BeautifulSoup(response.body_as_unicode())
        url = response.url
        url = url.replace('%20',' ').strip()

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
        for link in muster_links:
            work_code = link.split('workcode=')[1].split('&panchayat_code')[0]
            msr_no = link.split('msrno=')[1].split('&finyear')[0]
            par = urlparse.parse_qs(urlparse.urlparse(link).query)
            dt_from = par['dtfrm'][0]
            day = int(dt_from[0:2])
            month = int(dt_from[3:5])
            year = int(dt_from[6:])
            dt = datetime.datetime(year,month,day)
            with open(output_dir+'/dates.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow([dt_from,day,month,year,dt])
            if [work_code,msr_no] not in muster_roll_codes and dt>=datetime.datetime(2015,05,01):
                muster_roll_codes.append([work_code,msr_no])
                url = ('http://164.100.129.6/netnrega'+link[2:]).replace(';','').replace('%3b','').replace('-','%96').replace('%20','+').replace('!','')
                yield Request(url, self.handle_muster)
                

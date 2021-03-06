from fba_full_scrapy.items import JobcardItem
from fba_full_scrapy.items import MusterItem
import scrapy
from scrapy import Selector
from scrapy.http.request import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
import csv
import os
import datetime
from unidecode import unidecode
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
# from scrapy import signals
import sys
import urllib.parse
import pandas as pd
import logging

colors = {'active':['#00CC33','#D39027'],'inactive':['Red','Gray']} # to differentiate active and inactive jobcards, don't want redundant data
input_dir = './input' # specifying the input directory
gp_file = 'gp_list.csv' # initialized list of gram panchayats to download data from: district, block, panchayat, panchayat_code
output_dir = './full_output' # directory where the pertinent data gets stored
# create a new folder named full_output if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

fin_year = '2020-2021' # specify financial year

class JobcardSpider(CrawlSpider):
    name = "all_job_cards"
    
    # scrapy needs a list of start_urls, which populate_job_card_urls function populates
    def populate_job_card_urls():
        # specify options for headless browser for faster scraping of jobcard URLs
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_argument("start-maximized")
        chromeOptions.add_argument("enable-automation")
        chromeOptions.add_argument("--headless")
        chromeOptions.add_argument("--no-sandbox")
        chromeOptions.add_argument("--disable-infobars")
        chromeOptions.add_argument("--disable-dev-shm-usage")
        chromeOptions.add_argument("--disable-browser-side-navigation")
        chromeOptions.add_argument("--disable-gpu")
        # intitialize driver (Chrome)
        driver = webdriver.Chrome(ChromeDriverManager().install(), options = chromeOptions)

        start_urls = [] # initialize start_urls list, output by this function
        # opens input file, gp_file.csv, to extract jobcard URLs
        with open(input_dir + '/'+ gp_file, 'rU') as f:
            reader = csv.reader(f)
            # For each panchayat in csv, go to job card link
            for row_in in reader:
                if(row_in[0] == 'district_name'):
                    continue
                district = row_in[0]
                block = row_in[1]
                panchayat = row_in[2]
                panchayat_code = row_in[3]
                # plug and chug into URL
                url = 'http://mnregaweb2.nic.in/netnrega/IndexFrame.aspx?lflag=eng&district_name='+district+'&state_name=MADHYA+PRADESH&state_Code=17&block_name='+block+'&fin_year='+fin_year+'&check=1&panchayat_name='+panchayat+'(P)'+'&panchayat_code='+panchayat_code
                print(url)

                # following try/except block used to verify that this URL is reachable
                try:
                    driver.get(url)
                    driver.find_element_by_xpath(".//*[contains(text(), 'Job card/Employment Register')]").click()
                except:
                    print("Couldn't open directory for panchayat", panchayat)
                    continue
                
                soup = BeautifulSoup(driver.page_source, 'lxml')
                active_job_cards = []
                i = -1
                # Identify HH's in panchayat with active job cards
                try:
                    for tr in soup.find_all('table')[3].find_all('tr'):
                        i += 1
                        if i > 0:
                            color = tr.find_all('td')[2].find('font')['color']
                            if color in colors['inactive'] or color in colors['active']:
                                active_job_cards.append(tr.find_all('td')[1].text)
                except:
                    sys.exit("Couldn't parse the job card directory table for url {}".format(url))

                #Add active job card links to start url
                for item in active_job_cards:
                    job_card = item
                    url = 'http://mnregaweb2.nic.in/netnrega/state_html/jcr.aspx?reg_no='+job_card+'&panchayat_code='+panchayat_code+'&fin_year='+fin_year+'&digest=thissitesucks'
                    start_urls.append(url)
                    with open(output_dir+'/job_card_urls.csv', 'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([job_card, url])

        return start_urls

    def get_mr_tracker():
        if os.path.isfile(output_dir+'/encountered_muster_links.csv') and os.path.getsize(output_dir+'/encountered_muster_links.csv') > 0:
            mr_tracker = pd.read_csv(output_dir+'/encountered_muster_links.csv',
                                     header=None,names=['job_card', 'url', 'msr_no', 'muster_url', 'work_code'],
                                     usecols=['msr_no','work_code'],
                                     encoding='utf-8',
                                     dtype={'work_code':object,'msr_no':object})
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
    
        job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url']) # get the master list of job card urls to scrape

        jc_df = pd.merge(job_card_urls,jobcards.drop_duplicates(),how='left',left_on='job_card',right_on='job_card_number')
        
        jc_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']] # keep the job cards that haven't been scraped yet, drop duplicate job cards

        return jc_df

    #######################

    # start/restart business goes here
    
    if not os.path.isfile(output_dir+'/job_card_urls.csv'): # this is the first time through the scrape, need to populate the inital list of job card urls
        start_urls = populate_job_card_urls()
        mr_tracker = pd.DataFrame({'work_code': [], 'msr_no': []},
                                  dtype=object)

    else:  # need to see how far along in the scrape we are

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
        soup = BeautifulSoup(response.text, 'lxml')
        url = response.url
        url = url.replace('%20', ' ').strip()
        print(url)
        # Get top-level job card info
        par = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        print(par)
        panchayat = par['panchayat_code'][0]
        job_card = par['reg_no'][0]

        job_card_table = soup.find_all('table')[1]
        job_card_rows = job_card_table.find_all('tr')
        top_data = list()
        top_data = [str(panchayat),
                    unidecode(job_card_rows[2].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[3].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[5].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(soup.find_all('td')[13].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[6].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[7].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    '',
                    unidecode(job_card_rows[9].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[10].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[11].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[12].find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                    unidecode(job_card_rows[12].find_all('td')[3].text.encode('utf-8').decode('utf-8'))]

        bottom_data = list()

        if len(job_card_table.find_all('table')) == 1:
            item_data = top_data
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
            yield item
        
        else:
            person_table = job_card_table.find_all('table')[0]
            j = -1
            for tr in person_table:
                j += 1
                if j > 1 and j < len(person_table)-1:
                    # Add job card level and individual level data
                    bottom_data = [unidecode(tr.find_all('td')[1].text.encode('utf-8').decode('utf-8')),
                                   unidecode(tr.find_all('td')[2].text.encode('utf-8').decode('utf-8')),
                                   unidecode(tr.find_all('td')[3].text.encode('utf-8').decode('utf-8')),
                                   unidecode(tr.find_all('td')[4].text.encode('utf-8').decode('utf-8')),
                                   unidecode(tr.find_all('td')[5].text.encode('utf-8').decode('utf-8'))]
                    item_data = top_data + bottom_data
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
                    item['bank_po_name'] = item_data[17]
                    yield item

        print("DONE WITH ADDING ITEMS 😩😏˜")
        msr_table = soup.find('table', id="GridView3")
        if msr_table is None:
            print("msr_table DNE")
            pass
        else:
            msr_table = msr_table.find_all('tr')
            msr_no_col = []
            asset_link_col = []

            for row in msr_table:
                el_len = len(row.find_all('td'))
                if(el_len == 8):
                    msr_no = row.find_all('td')[5].text
                    asset_link = row.find_all('td')[5].a
                    if msr_no and asset_link:
                        asset_link = 'https://mnregaweb2.nic.in/netnrega/' + asset_link['href'][3:]

                        msr_no_col.append(msr_no.strip())
                        asset_link_col.append(asset_link)

            msr_df = pd.DataFrame(data={'msr_no': msr_no_col, 'link': asset_link_col})

            chromeOptions = webdriver.ChromeOptions()

            chromeOptions.add_argument("start-maximized")
            chromeOptions.add_argument("enable-automation")
            chromeOptions.add_argument("--headless")
            chromeOptions.add_argument("--no-sandbox")
            chromeOptions.add_argument("--disable-infobars")
            chromeOptions.add_argument("--disable-dev-shm-usage")
            chromeOptions.add_argument("--disable-browser-side-navigation")
            chromeOptions.add_argument("--disable-gpu")

            driver = webdriver.Chrome(ChromeDriverManager().install(), options = chromeOptions)

            for index, asset_link in msr_df.iterrows():
                msr_no = asset_link['msr_no']
                print("about to request MSR")
                #request = scrapy.Request(asset_link['link'],
                #              callback=self.parse_muster_links,
                #              cb_kwargs={'msr_no': asset_link['msr_no'], 'job_card': job_card, 'url': url})

                # print(request.cb_kwargs)

                driver.get(asset_link['link'])
                #msr_dict = request.meta['msr_dict']
                #work_code = msr_dict['work_code']
                #msr_link = msr_dict['msr_link']
                
                msr_link = driver.find_element_by_xpath("//a[contains(., "+msr_no+")]").get_attribute('href')
                print("msr_link:", msr_link)
                par = urllib.parse.parse_qs(urllib.parse.urlparse(msr_link).query)
                work_code = par['workcode'][0].encode('utf-8')

                print("work_code:", work_code)
                print("link:", msr_link)

                if not ((self.mr_tracker.msr_no == msr_no) & (self.mr_tracker.work_code == work_code)).any():

                    self.mr_tracker = self.mr_tracker.append({'work_code':work_code,'msr_no':msr_no},ignore_index=True)
                    print("doing the mr_tracker thing 🥶🥶🥶👹")
                    # muster_url = ('http://mnregaweb2.nic.in/netnrega'+link[2:]).replace(';', '').replace('%3b', '').replace('-', '%96').replace('%20', '+').replace('!', '')
                    with open(output_dir+'/encountered_muster_links.csv', 'a') as f:
                        print("about to write to CSV")
                        writer = csv.writer(f)
                        writer.writerow([job_card,
                                         url,
                                         msr_no,
                                         msr_link,
                                         work_code.decode('utf-8')])

        #def parse_muster_links(self, response, msr_no, job_card, url):
        #    print("in callback functioNONAIONDFIOAJDIOSJAOSDIJ")
            #msr_no = response.meta.get('msr_no')
            #job_card = response.meta.get('job_card')
            #url = response.meta.get('url')

            #msr_link = response.xpath("//a[contains(., "+msr_no+")]/@href").get()
            #msr_link = "https://mnregaweb2.nic.in/netnrega/"+msr_link[6:]
            #print("msr_link:", msr_link)

            #par = urllib.parse.parse_qs(urllib.parse.urlparse(msr_link.query))
            
            #work_code = par['workcode'][0].encode('utf-8')
            #dt_from = par['dtfrm'][0]
            #day = int(dt_from[0:2])
            #month = int(dt_from[3:5])
            #year = int(dt_from[6:])
            #dt = datetime.date(year, month, day)

            #msr_dict = response.cb_kwargs['msr_dict']
            #msr_dict['work_code'] = work_code
            #msr_dict['msr_link'] = msr_link

            #yield [msr_dict]

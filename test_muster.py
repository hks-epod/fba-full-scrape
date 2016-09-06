# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

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

url = 'http://164.100.129.6/netnrega/citizen_html/musternew.aspx?state_name=%e0%a4%ae%e0%a4%a7%e0%a5%8d%e0%a4%af+%e0%a4%aa%e0%a5%8d%e0%a4%b0%e0%a4%a6%e0%a5%87%e0%a4%b6+&district_name=%e0%a4%b6%e0%a5%8d%e0%a4%af%e0%a5%8b%e0%a4%aa%e0%a5%81%e0%a4%b0+%e0%a4%95%e0%a4%b2%e0%a4%be++&block_name=%e0%a4%ac%e0%a4%bf%e0%a4%9c%e0%a5%87%e0%a4%af%e0%a4%aa%e0%a5%81%e0%a4%b0+&panchayat_name=%e0%a4%85%e0%a4%97%e0%a4%b0%e0%a4%be+&workcode=1739001058/LD/22012034246616&panchayat_code=1739001058&msrno=66&finyear=2016%962017&dtfrm=02%2f04%2f2016&dtto=08%2f04%2f2016&wn=Riting%20bal%20nirman%20amala%20pant%20ke%20nechepalpur&id=1&referer=worker'
result = requests.get(url)

soup = BeautifulSoup(result.content, 'lxml')

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
        item = {}
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
        print item
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

            item = {}
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
            
            #print item
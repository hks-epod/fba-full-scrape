import pandas as pd
from email.mime.text import MIMEText
import smtplib
import sys
import os

def check_job_card_urls():
	msg = 'Checking the list of job card urls...\r\n'

	if os.path.isfile(output_dir+'/job_card_urls.csv') and os.path.getsize(output_dir+'/job_card_urls.csv') > 0:
		job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url'])
	else:
		job_card_urls = pd.DataFrame({'job_card':[],'url':[]})
	
	if os.path.isfile(gp_file) and os.path.getsize(gp_file) > 0:
		gp_list = pd.read_csv(gp_file,header=None,names=['district_name','district_code','block_name','block_code','panchayat_name','panchayat_code','treatment_status'],dtype={'panchayat_code':object})
	else:
		sys.exit('GP input file not found or empty')

	job_card_urls['panchayat_code'] = job_card_urls.url.apply(lambda x: x.split('Panchayat_Code=')[1].split('&')[0])
	job_card_urls = job_card_urls[['panchayat_code','job_card']].drop_duplicates().groupby(['panchayat_code']).count().reset_index()

	job_card_counts = pd.merge(gp_list[['panchayat_code']],job_card_urls,how='left',on='panchayat_code')

	job_card_counts = job_card_counts.fillna(0)

	if len(job_card_counts[job_card_counts.job_card==0].index)==0:
		msg += 'List of job card urls was populated for all panchayats\r\n'
	else:
		msg += 'WARNING: list of job card urls doesn\'t contain all the study panchayats\r\nNeed to restart the scrape\r\n\r\n'
		msg += job_card_counts[job_card_counts.job_card==0].to_string()
		msg += '\r\n'

	msg += '\r\n'

	return msg


def check_job_card_scrape():
	msg = 'Checking the progress of the job card scrape against the list of job card urls...\r\n'

	if os.path.isfile(output_dir+'/jobcard.csv') and os.path.getsize(output_dir+'/jobcard.csv') > 0:
		jobcards = pd.read_csv(output_dir+'/jobcard.csv',encoding='utf-8',usecols=['job_card_number'],dtype={'job_card_number':object})
		jobcards = jobcards[jobcards['job_card_number']!='job_card_number'] # Headers get appended every time the scraper runs
	else:
		jobcards = pd.DataFrame({'job_card_number':[]},dtype=object)
	
	if os.path.isfile(output_dir+'/job_card_urls.csv') and os.path.getsize(output_dir+'/job_card_urls.csv') > 0:
		job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url'])
	else:
		job_card_urls = pd.DataFrame({'job_card':[],'url':[]})
	
	jc_df = pd.merge(job_card_urls,jobcards.drop_duplicates(),how='left',left_on='job_card',right_on='job_card_number')

	jc_notscraped_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']]

	if len(jc_notscraped_df.index)==0:
		jc_total = len(jc_df.index)
		msg += 'All {} of the job cards have been scraped\r\n'.format(jc_total)
	else:
		jc_total = len(jc_df.index)
		jc_scraped = jc_total - len(jc_notscraped_df.index)
		jc_pct = (float(jc_scraped)/float(jc_total))*100
		msg += '{} of {} job cards have been scraped ({:.1f}%)\r\n'.format(jc_scraped,jc_total,jc_pct)

	msg += '\r\n'
	return msg

def check_muster_scrape():
	msg = 'Checking the progress of the muster roll scrape against the list of encountered muster urls...\r\n'
	msg += 'Note: list of encountered muster roll urls is populated from the job card pages and will grow until all job cards are scraped\r\n'

	if os.path.isfile(output_dir+'/muster.csv') and os.path.getsize(output_dir+'/muster.csv') > 0:
		musters = pd.read_csv(output_dir+'/muster.csv',encoding='utf-8',usecols=['work_code','msr_no'],dtype={'work_code':object,'msr_no':object})
		musters = musters[musters.work_code!='work_code'] # when the script restarts it puts in an extra header row
	else:
		musters = pd.DataFrame({'work_code':[],'msr_no':[]},dtype=object)
	
	musters['right'] = 1

	# Find all the musters that haven't been scraped
	if os.path.isfile(output_dir+'/encountered_muster_links.csv'):
		encountered_muster_links = pd.read_csv(output_dir+'/encountered_muster_links.csv',header=None,names=['job_card', 'url', 'msr_no', 'muster_url', 'work_code'],usecols=['msr_no','work_code','muster_url'],encoding='utf-8',dtype={'work_code':object,'msr_no':object,'muster_url':object})
	else:
		encountered_muster_links = pd.DataFrame({'msr_no':[], 'muster_url':[], 'work_code':[]},dtype=object)

	mr_df = pd.merge(encountered_muster_links,musters.drop_duplicates(),how='left',on=['msr_no','work_code'])

	mr_notscraped_df = mr_df[pd.isnull(mr_df.right)] # keep the musters that haven't been scraped yet

	if len(mr_notscraped_df.index)==0:
		mr_total = len(mr_df.index)
		msg += 'All {} of the encountered muster roll urls have been scraped\r\n'.format(mr_total)
	else:
		mr_total = len(mr_df.index)
		mr_scraped = mr_total - len(mr_notscraped_df.index)
		mr_pct = (float(mr_scraped)/float(mr_total))*100
		msg += '{} of {} encountered muster roll urls have been scraped ({:.1f}%)\r\n'.format(mr_scraped,mr_total,mr_pct)

	msg += '\r\n'
	
	return msg

def send_email(email_recipients,msg_string):

	msg = MIMEText(msg_string)
	msg['Subject'] = 'FBA Scrape Progress'
	msg['From'] = 'python@python.com'
	msg['To'] = ','.join(email_recipients)
	s = smtplib.SMTP('localhost')
	s.sendmail('test@test.com', email_recipients, msg.as_string())
	s.quit()

if __name__ == '__main__':

	input_dir = './input'
	output_dir = './full_output'

	gp_file = input_dir + '/gp list.csv'
	
	email_recipients = [
		'edodge11@gmail.com',
		# 'simone.schaner@dartmouth.edu',
		# 'Patrick_Agte@hks.harvard.edu'
	]

	msg_string = ''

	msg_string += check_job_card_urls()
	msg_string += check_job_card_scrape()
	msg_string += check_muster_scrape()

	# print msg_string
	send_email(email_recipients,msg_string)


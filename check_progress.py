import pandas as pd
from email.mime.text import MIMEText
import smtplib

input_dir = './input'
output_dir = './full_output'

def check_job_card_urls():
	msg = 'Checking the list of job card urls...\r\n'

	job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url'])
	gp_list = pd.read_csv(input_dir+'/gp list.csv'header=None,names=['district_name','district_code','block_name','block_code','panchayat_name','panchayat_code','treatment_status'])

	job_card_urls['panchayat_code'] = job_card_urls.url.apply(lambda x: x.split('Panchayat_Code=')[1].split('&')[0])
	job_card_urls = job_card_urls[['panchayat_code','job_card']].drop_duplicates().groupby(['panchayat_code']).count().reset_index()

	job_card_counts = pd.merge(gp_list[['panchayat_code']],jobcards[['job_card_number']].drop_duplicates(),how='left',on='panchayat_code')

	job_card_counts = job_card_counts.fillna(0)

	if len(job_card_counts[job_card_counts.count==0].index)==0:
		msg += 'List of job card urls was populated for all panchayats\r\n'
	else:
		msg += 'WARNING: list of job card urls doesn\'t contain all the study panchayats\r\nNeed to restart the scrape\r\n\r\n'
		msg += job_card_counts[job_card_counts.count==0].to_string()
		msg += '\r\n'

	msg += '\r\n'

	return msg


def check_job_card_scrape():
	msg = 'Checking the progress of the job card scrape against the list of job card urls...\r\n'
	
	jobcards = pd.read_csv(output_dir+'/jobcard.csv')
	job_card_urls = pd.read_csv(output_dir+'/job_card_urls.csv',header=None,names=['job_card','url'])
	jc_df = pd.merge(job_card_urls,jobcards[['job_card_number']].drop_duplicates(),how='left',on_left='job_card',on_right='job_card_number')
	
	jc_notscraped_df = jc_df[pd.isnull(jc_df.job_card_number)][['job_card','url']]

	if len(jc_notscraped_df.index)==0:
		msg += 'All the job cards have been scraped\r\n'
	else:
		jc_total = len(jc_df.index)
		jc_scraped = jc_total - len(jc_notscraped_df.index)
		jc_pct = "%.1f" % (float(jc_scraped)/float(jc_total))*100
		msg += '%d of %d job cards have been scraped (%s%)\r\n' % (jc_scraped,jc_total,jc_pct)

	msg += '\r\n'
	return msg

def check_muster_scrape():
	msg = 'Checking the progress of the muster roll scrape against the list of encountered muster urls...\r\n'
	msg += 'Note: list of encountered muster urls is populated from the job card pages and will grow until all job cards are scraped\r\n'

	musters = pd.read_csv(output_dir+'/muster.csv')

    # Find all the musters that haven't been scraped
    encountered_muster_links = pd.read_csv(output_dir+'/encountered_muster_links.csv',header=None,names=['job_card', 'url', 'msr_no', 'muster_url'])

    mr_df = pd.merge(encountered_muster_links,musters[['mr_no']].drop_duplicates(),how='left',on_left='msr_no',on_right='mr_no')
    mr_not_scraped_df = mr_df[pd.isnull(mr_df.mr_no)].drop_duplicates(subset=['msr_no']) # keep the musters that haven't been scraped yet, drop duplicate musters

	if len(mr_notscraped_df.index)==0:
		msg += 'All the encountered muster roll urls have been scraped\r\n'
	else:
		mr_total = len(mr_df.index)
		mr_scraped = mr_total - len(mr_notscraped_df.index)
		mr_pct = "%.1f" % (float(mr_scraped)/float(mr_total))*100
		msg += '%d of %d encountered muster urls have been scraped (%s%)\r\n' % (mr_scraped,mr_total,mr_pct)

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

	email_recipients = [
		'edodge11@gmail.com',
		'simone.schaner@dartmouth.edu',
		'Patrick_Agte@hks.harvard.edu'
	]

	msg_string = ''

	msg_string += check_job_card_urls()
	msg_string += check_job_card_scrape()
	msg_string += check_muster_scrape()

	send_email(email_recipients,msg_string)

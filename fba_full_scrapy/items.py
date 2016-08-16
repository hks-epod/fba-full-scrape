# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class JobcardItem(scrapy.Item):
    panchayat_code = scrapy.Field()
    job_card_number = scrapy.Field()
    head_of_hh_name = scrapy.Field()
    father_husband_name = scrapy.Field()
    sc_st_category = scrapy.Field()
    reg_date = scrapy.Field()
    address = scrapy.Field()
    village_name = scrapy.Field()
    panchayat_name = scrapy.Field()
    block_name = scrapy.Field()
    district_name = scrapy.Field()
    bpl_status = scrapy.Field()
    family_id = scrapy.Field()
    person_id = scrapy.Field()
    applicant_name = scrapy.Field()
    applicant_gender = scrapy.Field()
    applicant_age = scrapy.Field()
    account_no = scrapy.Field()
    bank_po_name = scrapy.Field()
    aadhar_no = scrapy.Field()
    pass

class MusterItem(scrapy.Item):
    panchayat_code = scrapy.Field()
    job_card_number = scrapy.Field()
    worker_name = scrapy.Field()
    aadhar_no = scrapy.Field()
    sc_st_category = scrapy.Field()
    village_name = scrapy.Field()
    present = scrapy.Field()
    days_worked = scrapy.Field()
    average_daily_wage = scrapy.Field()
    dues = scrapy.Field()
    travel_food_expenses = scrapy.Field()
    tool_payments = scrapy.Field()
    total_cash_payments = scrapy.Field()
    account_no = scrapy.Field()
    bank_po_name = scrapy.Field()
    po_code_branch_name = scrapy.Field()
    po_address_branch_code = scrapy.Field()
    wagelist_no = scrapy.Field()
    status = scrapy.Field()
    payment_date = scrapy.Field()
    signature = scrapy.Field()
    ac_credited_date = scrapy.Field()
    msr_no = scrapy.Field()
    work_start_date = scrapy.Field()
    work_end_date = scrapy.Field()
    work_approval_date = scrapy.Field()
    work_code = scrapy.Field()
    work_name = scrapy.Field()
    pass

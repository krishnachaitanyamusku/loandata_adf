# Databricks notebook source
spark.sql("""
create or replace view loandata_catalog.loandata_silver_schema.customers_loan_join as select
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.joint_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths

FROM loandata_catalog.loandata_silver_schema.customers_data c
LEFT JOIN loandata_catalog.loandata_silver_schema.loans_data l on c.member_id = l.member_id
LEFT JOIN loandata_catalog.loandata_silver_schema.loan_repayment r ON l.loan_id = r.loan_id
LEFT JOIN loandata_catalog.loandata_silver_schema.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq e ON c.member_id = e.member_id
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.customers_loan_join").display()
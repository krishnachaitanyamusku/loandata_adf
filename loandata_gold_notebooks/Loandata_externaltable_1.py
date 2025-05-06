# Databricks notebook source
customers_data_df = spark.read \
.format("delta") \
.load("abfss://silver-layer@loandatasa.dfs.core.windows.net/customers_data/")

# COMMAND ----------

customers_data_df.display()

# COMMAND ----------

customers_data_df.printSchema()

# COMMAND ----------

spark.sql("create database itv006277_lending_club")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE loandata_catalog.loandata_silver_schema.customers_data (
# MAGIC   member_id STRING,
# MAGIC   emp_title STRING,
# MAGIC   emp_length INT, 
# MAGIC   home_ownership STRING,
# MAGIC   annual_income FLOAT,
# MAGIC   address_state STRING,
# MAGIC   address_zipcode STRING,
# MAGIC   address_country STRING,
# MAGIC   grade STRING, 
# MAGIC   sub_grade STRING,
# MAGIC   verification_status STRING,
# MAGIC   total_high_credit_limit FLOAT,
# MAGIC   application_type STRING,
# MAGIC   joint_annual_income FLOAT, 
# MAGIC   verification_status_joint STRING,
# MAGIC   ingest_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver-layer@loandatasa.dfs.core.windows.net/customers_data/'

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.customers_data").display()

# COMMAND ----------

spark.sql("""
create table loandata_catalog.loandata_silver_schema.loans_data(
loan_id string, member_id string, loan_amount float, funded_amount float,
loan_term_years integer, interest_rate float, monthly_installment float, issue_date string,
loan_status string, loan_purpose string, loan_title string, ingest_date timestamp)
using delta
location 'abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_data/'
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.loans_data").display()

# COMMAND ----------

loan_repayment_df = spark.read \
.format("delta") \
.load("abfss://silver-layer@loandatasa.dfs.core.windows.net/loan_repayment/")

# COMMAND ----------

loan_repayment_df.display()

# COMMAND ----------

loan_repayment_df.printSchema()

# COMMAND ----------

spark.sql("""CREATE TABLE loandata_catalog.loandata_silver_schema.loan_repayment(loan_id BIGINT, total_principal_received float,
total_interest_received float,total_late_fee_received float,total_payment_received float,last_payment_amount float,
last_payment_date string,next_payment_date string,ingest_date timestamp)
using DELTA LOCATION 'abfss://silver-layer@loandatasa.dfs.core.windows.net/loan_repayment'
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.loan_repayment").display()

# COMMAND ----------

loans_defaulters_delinq_df = spark.read \
.format("delta") \
.load("abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters_deling_delta/")

# COMMAND ----------

loans_defaulters_delinq_df.printSchema()

# COMMAND ----------

loans_defaulters_delinq_df.display()

# COMMAND ----------

spark.sql("""CREATE TABLE loandata_catalog.loandata_silver_schema.loans_defaulters_delinq(
member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
using DELTA LOCATION 'abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters_deling_delta/'""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.loans_defaulters_delinq").display()

# COMMAND ----------

spark.sql("""CREATE TABLE loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq(
member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
using DELTA LOCATION 'abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_def_detail_records_enq_df_delta'""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq").display()

# COMMAND ----------


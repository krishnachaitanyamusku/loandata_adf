# Databricks notebook source
spark.sql("""select member_id, count(*) as total 
from loandata_catalog.loandata_silver_schema.customers_data
group by member_id order by total desc
""").display()

# COMMAND ----------

spark.sql("""select * from loandata_catalog.loandata_silver_schema.customers_data
where member_id like 'e4c167053d5418230%'""").display()

# COMMAND ----------

spark.sql("""select member_id, count(*) as total 
from loandata_catalog.loandata_silver_schema.loans_defaulters_delinq
group by member_id order by total desc
""").display()

# COMMAND ----------

spark.sql("""select * from loandata_catalog.loandata_silver_schema.loans_defaulters_delinq
where member_id like 'e4c167053d5418230%'""").display()

# COMMAND ----------

spark.sql("""select member_id, count(*) as total 
from loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq
group by member_id order by total desc
""").display()

# COMMAND ----------

spark.sql("""select * from loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq
where member_id like 'e4c167053d5418230%'""").display()

# COMMAND ----------

bad_data_customer_df = spark.sql("""select member_id from(select member_id, count(*)
as total from loandata_catalog.loandata_silver_schema.customers_data
group by member_id having total > 1)""")

# COMMAND ----------

bad_data_customer_df.display()

# COMMAND ----------

bad_data_customer_df.count()

# COMMAND ----------

bad_data_loans_defaulters_delinq_df = spark.sql("""select member_id from(select member_id, count(*)
as total from loandata_catalog.loandata_silver_schema.loans_defaulters_delinq
group by member_id having total > 1)""")

# COMMAND ----------

bad_data_loans_defaulters_delinq_df.count()

# COMMAND ----------

bad_data_loans_defaulters_delinq_df.display()

# COMMAND ----------

bad_data_loans_defaulters_detail_rec_enq_df = spark.sql("""select member_id from(select member_id, count(*)
as total from loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq
group by member_id having total > 1)""")

# COMMAND ----------

bad_data_loans_defaulters_detail_rec_enq_df.count()

# COMMAND ----------

bad_data_loans_defaulters_detail_rec_enq_df.display()

# COMMAND ----------

bad_data_customer_df.repartition(1).write \
.format("delta") \
.option("header", True) \
.mode("overwrite") \
.option("path", "abfss://baddata@loandatasa.dfs.core.windows.net/customers_bad_data/") \
.save()

# COMMAND ----------

bad_data_loans_defaulters_delinq_df.repartition(1).write \
.format("delta") \
.option("header", True) \
.mode("overwrite") \
.option("path", "abfss://baddata@loandatasa.dfs.core.windows.net/bad_data_loans_defaulters_delinq/") \
.save()

# COMMAND ----------

bad_data_loans_defaulters_detail_rec_enq_df.repartition(1).write \
.format("delta") \
.option("header", True) \
.mode("overwrite") \
.option("path", "abfss://baddata@loandatasa.dfs.core.windows.net/bad_data_loans_defaulters_detail_rec_enq/") \
.save()

# COMMAND ----------

bad_customer_data_df = bad_data_customer_df.select("member_id") \
.union(bad_data_loans_defaulters_delinq_df.select("member_id")) \
.union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))

# COMMAND ----------

bad_customer_data_final_df = bad_customer_data_df.distinct()

# COMMAND ----------

bad_customer_data_final_df.count()

# COMMAND ----------

bad_customer_data_final_df.repartition(1).write \
.format("delta") \
.option("header", True) \
.mode("overwrite") \
.option("path", "abfss://baddata@loandatasa.dfs.core.windows.net/bad_customer_data_final") \
.save()

# COMMAND ----------

bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# COMMAND ----------

customers_cleaned_df = spark.sql("""select * from loandata_catalog.loandata_silver_schema.customers_data
where member_id NOT IN (select member_id from bad_data_customer)
""")

# COMMAND ----------

customers_cleaned_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/customers_cleaned_delta") \
.save()

# COMMAND ----------

loans_defaulters_delinq_df = spark.sql("""select * from loandata_catalog.loandata_silver_schema.loans_defaulters_delinq
where member_id NOT IN (select member_id from bad_data_customer)
""")

# COMMAND ----------

loans_defaulters_delinq_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/loans_defaulters_delinq_delta") \
.save()

# COMMAND ----------

loans_defaulters_detail_rec_enq_df = spark.sql("""select * from loandata_catalog.loandata_silver_schema.loans_defaulters_detail_rec_enq
where member_id NOT IN (select member_id from bad_data_customer)
""")

# COMMAND ----------

loans_defaulters_detail_rec_enq_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/loans_defaulters_detail_rec_enq_delta") \
.save()

# COMMAND ----------

spark.sql("""
create TABLE loandata_catalog.loandata_gold_schema.customers_cleaned(member_id string, emp_title string, emp_length int, home_ownership string, 
annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
sub_grade string, verification_status string, total_high_credit_limit float, application_type string, 
joint_annual_income float, verification_status_joint string, ingest_date timestamp)
using delta
LOCATION 'abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/customers_cleaned_delta/'
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_gold_schema.customers_cleaned").display()

# COMMAND ----------

spark.sql("""
create TABLE loandata_catalog.loandata_gold_schema.loans_defaulters_delinq_cleaned(member_id string,delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
using delta
LOCATION 'abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/loans_defaulters_delinq_delta'
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_gold_schema.loans_defaulters_delinq_cleaned").display()

# COMMAND ----------

spark.sql("""
create TABLE loandata_catalog.loandata_gold_schema.loans_defaulters_detail_rec_enq_cleaned(member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
using delta
LOCATION 'abfss://gold-layer@loandatasa.dfs.core.windows.net/cleaned/loans_defaulters_detail_rec_enq_delta'
""")

# COMMAND ----------

spark.sql("select * from loandata_catalog.loandata_gold_schema.loans_defaulters_detail_rec_enq_cleaned").display()

# COMMAND ----------

spark.sql("""select member_id, count(*) as total 
from loandata_catalog.loandata_gold_schema.customers_cleaned
group by member_id order by total desc""").display()
# Databricks notebook source
# MAGIC %md
# MAGIC ## Associating points to the grades in order to calculate the Loan Score

# COMMAND ----------

unacceptable_rated_pts = 0
very_bad_rated_pts = 100
bad_rated_pts = 250
good_rated_pts = 500
very_good_rated_pts = 650
excellent_rated_pts = 800

# COMMAND ----------

unacceptable_grade_pts = 750
very_bad_grade_pts = 1000
bad_grade_pts = 1500
good_grade_pts = 2000
very_good_grade_pts = 2500

# COMMAND ----------

# MAGIC %md
# MAGIC ## The tables required to calculate the Loan Score

# COMMAND ----------

# MAGIC %md
# MAGIC #####customers_cleaned
# MAGIC
# MAGIC #####loans
# MAGIC
# MAGIC #####loans_repayments
# MAGIC
# MAGIC #####loans_defaulters_delinq_cleaned
# MAGIC
# MAGIC #####loans_defaulters_detail_red_enq_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loan Score Calculation Criteria 1: Payment History(ph)

# COMMAND ----------

bad_customer_data_final_df = spark.read \
.format("delta") \
.option("header", True) \
.load("abfss://baddata@loandatasa.dfs.core.windows.net/bad_customer_data_final/")

# COMMAND ----------

bad_customer_data_final_df.display()

# COMMAND ----------

bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# COMMAND ----------

query = f"""
SELECT c.member_id,
    CASE
        WHEN p.last_payment_amount < (c.monthly_installment * 0.5) THEN {very_bad_rated_pts}
        WHEN p.last_payment_amount >= (c.monthly_installment * 0.5) AND p.last_payment_amount < c.monthly_installment THEN {very_bad_rated_pts}
        WHEN p.last_payment_amount = c.monthly_installment THEN {good_rated_pts}
        WHEN p.last_payment_amount > c.monthly_installment AND p.last_payment_amount <= (c.monthly_installment * 1.5) THEN {very_good_rated_pts}
        WHEN p.last_payment_amount > (c.monthly_installment * 1.5) THEN {excellent_rated_pts}
        ELSE {unacceptable_rated_pts}
    END AS last_payment_pts,
    CASE
        WHEN p.total_payment_received >= (c.funded_amount * 0.5) THEN {very_good_rated_pts}
        WHEN p.total_payment_received < (c.funded_amount * 0.5) AND p.total_payment_received > 0 THEN {good_rated_pts}
        WHEN p.total_payment_received = 0 OR p.total_payment_received IS NULL THEN {unacceptable_rated_pts}
    END AS total_payment_pts
FROM loandata_catalog.loandata_silver_schema.loan_repayment p
INNER JOIN loandata_catalog.loandata_silver_schema.loans_data c
    ON c.loan_id = p.loan_id
WHERE c.member_id NOT IN (SELECT member_id FROM bad_data_customer)
"""

payment_history_df = spark.sql(query)


# COMMAND ----------

payment_history_df.display()

# COMMAND ----------

payment_history_df.createOrReplaceTempView("payment_history_pts")

# COMMAND ----------

spark.sql("select * from payment_history_pts").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loan Score Calculation Criteria 2: Loan Defaulters History(ldh)

# COMMAND ----------

ldh_ph_query = f"""
SELECT p.*,
    CASE
        WHEN d.delinq_2yrs = 0 THEN {excellent_rated_pts}
        WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN {bad_rated_pts}
        WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN {very_bad_rated_pts}
        WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN {unacceptable_rated_pts}
    END AS delinq_pts,
    CASE
        WHEN l.pub_rec = 0 THEN {excellent_rated_pts}
        WHEN l.pub_rec BETWEEN 1 AND 2 THEN {bad_rated_pts}
        WHEN l.pub_rec BETWEEN 3 AND 5 THEN {very_bad_rated_pts}
        WHEN l.pub_rec > 5 OR l.pub_rec IS NULL THEN {very_bad_rated_pts}
    END AS public_records_pts,
    CASE
        WHEN l.pub_rec_bankruptcies = 0 THEN {excellent_rated_pts}
        WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN {bad_rated_pts}
        WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN {very_bad_rated_pts}
        WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL THEN {very_bad_rated_pts}
    END AS public_bankruptcies_pts,
    CASE
        WHEN l.inq_last_6mths = 0 THEN {excellent_rated_pts}
        WHEN l.inq_last_6mths BETWEEN 1 AND 2 THEN {bad_rated_pts}
        WHEN l.inq_last_6mths BETWEEN 3 AND 5 THEN {very_bad_rated_pts}
        WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL THEN {unacceptable_rated_pts}
    END AS enq_pts
FROM loandata_catalog.loandata_gold_schema.loans_defaulters_detail_rec_enq_cleaned l
INNER JOIN loandata_catalog.loandata_gold_schema.loans_defaulters_delinq_cleaned d ON d.member_id = l.member_id
INNER JOIN payment_history_pts p ON p.member_id = l.member_id
WHERE l.member_id NOT IN (SELECT member_id FROM bad_data_customer)
"""

loan_defaulters_history_df = spark.sql(ldh_ph_query)



# COMMAND ----------

loan_defaulters_history_df.createOrReplaceTempView("ldh_ph_pts")

# COMMAND ----------

spark.sql("select * from ldh_ph_pts").display()

# COMMAND ----------

fh_ldh_ph_query = f"""
SELECT ldef.*,
    CASE
        WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN {excellent_rated_pts}
        WHEN LOWER(l.loan_status) LIKE '%current%' THEN {good_rated_pts}
        WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN {bad_rated_pts}
        WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN {very_bad_rated_pts}
        WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN {unacceptable_rated_pts}
        ELSE {unacceptable_rated_pts}
    END AS loan_status_pts,

    CASE
        WHEN LOWER(a.home_ownership) LIKE '%own' THEN {excellent_rated_pts}
        WHEN LOWER(a.home_ownership) LIKE '%rent' THEN {good_rated_pts}
        WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN {bad_rated_pts}
        WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN {very_bad_rated_pts}
    END AS home_pts,

    CASE
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN {excellent_rated_pts}
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN {very_good_rated_pts}
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN {good_rated_pts}
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN {bad_rated_pts}
        WHEN l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN {very_bad_rated_pts}
        ELSE {unacceptable_rated_pts}
    END AS credit_limit_pts,

    CASE
        WHEN a.grade = 'A' AND a.sub_grade = 'A1' THEN {excellent_rated_pts}
        WHEN a.grade = 'A' AND a.sub_grade = 'A2' THEN {excellent_rated_pts * 0.95}
        WHEN a.grade = 'A' AND a.sub_grade = 'A3' THEN {excellent_rated_pts * 0.90}
        WHEN a.grade = 'A' AND a.sub_grade = 'A4' THEN {excellent_rated_pts * 0.85}
        WHEN a.grade = 'A' AND a.sub_grade = 'A5' THEN {excellent_rated_pts * 0.80}

        WHEN a.grade = 'B' AND a.sub_grade = 'B1' THEN {very_good_rated_pts}
        WHEN a.grade = 'B' AND a.sub_grade = 'B2' THEN {very_good_rated_pts * 0.95}
        WHEN a.grade = 'B' AND a.sub_grade = 'B3' THEN {very_good_rated_pts * 0.90}
        WHEN a.grade = 'B' AND a.sub_grade = 'B4' THEN {very_good_rated_pts * 0.85}
        WHEN a.grade = 'B' AND a.sub_grade = 'B5' THEN {very_good_rated_pts * 0.80}

        WHEN a.grade = 'C' AND a.sub_grade = 'C1' THEN {good_rated_pts}
        WHEN a.grade = 'C' AND a.sub_grade = 'C2' THEN {good_rated_pts * 0.95}
        WHEN a.grade = 'C' AND a.sub_grade = 'C3' THEN {good_rated_pts * 0.90}
        WHEN a.grade = 'C' AND a.sub_grade = 'C4' THEN {good_rated_pts * 0.85}
        WHEN a.grade = 'C' AND a.sub_grade = 'C5' THEN {good_rated_pts * 0.80}

        WHEN a.grade = 'D' AND a.sub_grade = 'D1' THEN {bad_rated_pts}
        WHEN a.grade = 'D' AND a.sub_grade = 'D2' THEN {bad_rated_pts * 0.95}
        WHEN a.grade = 'D' AND a.sub_grade = 'D3' THEN {bad_rated_pts * 0.90}
        WHEN a.grade = 'D' AND a.sub_grade = 'D4' THEN {bad_rated_pts * 0.85}
        WHEN a.grade = 'D' AND a.sub_grade = 'D5' THEN {bad_rated_pts * 0.80}

        WHEN a.grade = 'E' AND a.sub_grade = 'E1' THEN {very_bad_rated_pts}
        WHEN a.grade = 'E' AND a.sub_grade = 'E2' THEN {very_bad_rated_pts * 0.95}
        WHEN a.grade = 'E' AND a.sub_grade = 'E3' THEN {very_bad_rated_pts * 0.90}
        WHEN a.grade = 'E' AND a.sub_grade = 'E4' THEN {very_bad_rated_pts * 0.85}
        WHEN a.grade = 'E' AND a.sub_grade = 'E5' THEN {very_bad_rated_pts * 0.80}

        WHEN a.grade IN ('F', 'G') THEN {unacceptable_rated_pts}
    END AS grade_pts

FROM ldh_ph_pts ldef
INNER JOIN loandata_catalog.loandata_silver_schema.loans_data l ON ldef.member_id = l.member_id
INNER JOIN loandata_catalog.loandata_gold_schema.customers_cleaned a ON a.member_id = ldef.member_id
WHERE ldef.member_id NOT IN (SELECT member_id FROM bad_data_customer)
"""

fh_ldh_ph_df = spark.sql(fh_ldh_ph_query)


# COMMAND ----------

fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

# COMMAND ----------

spark.sql("select * from fh_ldh_ph_pts").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final loan score calculation by considering all the 3 criterias with the following %**

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Payment History = 20%
# MAGIC #### 2. Loan Defaults = 45%
# MAGIC #### 3. Financial Health = 35%

# COMMAND ----------

loan_score = spark.sql("""SELECT member_id, \
((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, \
((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, \
((loan_status_pts + home_pts + credit_limit_pts + grade_pts)*0.35) as financial_health_pts \
FROM fh_ldh_ph_pts""")

# COMMAND ----------

loan_score.display()

# COMMAND ----------

final_loan_score = loan_score.withColumn('loan_score', loan_score.payment_history_pts + loan_score.defaulters_history_pts + loan_score.financial_health_pts)

# COMMAND ----------

final_loan_score.createOrReplaceTempView("loan_score_eval")

# COMMAND ----------

loan_score_query = f"""
SELECT ls.*,
  CASE
    WHEN loan_score > {very_good_grade_pts} THEN 'A'
    WHEN loan_score <= {very_good_grade_pts} AND loan_score > {good_grade_pts} THEN 'B'
    WHEN loan_score <= {good_grade_pts} AND loan_score > {bad_grade_pts} THEN 'C'
    WHEN loan_score <= {bad_grade_pts} AND loan_score > {very_bad_grade_pts} THEN 'D'
    WHEN loan_score <= {very_bad_grade_pts} AND loan_score > {unacceptable_grade_pts} THEN 'E'
    WHEN loan_score <= {unacceptable_grade_pts} THEN 'F'
  END AS loan_final_grade
FROM loan_score_eval ls
"""

loan_score_final = spark.sql(loan_score_query)


# COMMAND ----------

loan_score_final.createOrReplaceTempView("loan_final_table")

# COMMAND ----------

spark.sql("select * from loan_final_table where loan_final_grade in ('C')").display()

# COMMAND ----------

spark.sql("select count(*) from loan_final_table")

# COMMAND ----------

loan_score_final.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://gold-layer@loandatasa.dfs.core.windows.net/loan_score/loan_score_final_delta") \
.save()
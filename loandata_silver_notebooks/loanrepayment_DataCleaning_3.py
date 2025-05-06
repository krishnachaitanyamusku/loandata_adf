# Databricks notebook source
Loans_repay_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.option("inferSchema", True) \
.load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/loan_repayment/loan_repayment.csv")

# COMMAND ----------

loans_repay_raw_df.display()

# COMMAND ----------

loans_repay_raw_df.printSchema()

# COMMAND ----------

loans_repay_schema = 'loan_id bigint, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'

# COMMAND ----------

loans_repay_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loans_repay_schema) \
.load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/loan_repayment/loan_repayment.csv")

# COMMAND ----------

loans_repay_raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date", current_timestamp())

# COMMAND ----------

loans_repay_df_ingestd.display()

# COMMAND ----------

loans_repay_df_ingestd.printSchema()

# COMMAND ----------

loans_repay_df_ingestd.count()

# COMMAND ----------

loans_repay_df_ingestd.createOrReplaceTempView("loan_repayments")

# COMMAND ----------

spark.sql("select count(*) from loan_repayments where total_principal_received is null")

# COMMAND ----------

columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]

# COMMAND ----------

loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)

# COMMAND ----------

loans_repay_filtered_df.count()

# COMMAND ----------

loans_repay_filtered_df.createOrReplaceTempView("loan_repayments")

# COMMAND ----------

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0").show()

# COMMAND ----------

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0").show()

# COMMAND ----------

spark.sql("select * from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0").display()

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
   "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
)

# COMMAND ----------

loans_payments_fixed_df.display()

# COMMAND ----------

loans_payments_fixed_df.filter("loan_id == '1064185'").display()

# COMMAND ----------

loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")

# COMMAND ----------

loans_payments_fixed2_df.count()

# COMMAND ----------

loans_payments_fixed2_df.createOrReplaceTempView("loan_repayments_fixed2")

# COMMAND ----------

spark.sql("select next_payment_date,count(*) as total from loan_repayments_fixed2 group by next_payment_date order by total desc").display()

# COMMAND ----------

spark.sql("select last_payment_date,count(*) as total from loan_repayments_fixed2 group by last_payment_date order by total desc").display()

# COMMAND ----------

loans_payments_fixed2_df.filter("last_payment_date = '0.0'").count()

# COMMAND ----------

loans_payments_fixed2_df.filter("next_payment_date = '0.0'").count()

# COMMAND ----------

loans_payments_fixed2_df.filter("last_payment_date is null").count()

# COMMAND ----------

loans_payments_fixed2_df.filter("next_payment_date is null").count()

# COMMAND ----------

from pyspark.sql.functions import when, col, to_date

loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
    "last_payment_date",
    when((col("last_payment_date") == "0.0") | (col("last_payment_date").isNull()), None) \
    .otherwise(col("last_payment_date"))
)

# COMMAND ----------

from pyspark.sql.functions import when, to_date, col

loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
    "next_payment_date",
    when(
        (col("next_payment_date") == "0.0") | (col("next_payment_date").isNull()), None) \
        .otherwise(col("next_payment_date"))
)

# COMMAND ----------

loans_payments_ndate_fixed_df.filter(col("next_payment_date") == '0.0').count()

# COMMAND ----------

spark.sql("select next_payment_date,count(*) as total from loan_repayments_Ndate_fixed group by next_payment_date order by total desc").display()

# COMMAND ----------

import pyspark.sql.functions as F

valid_dates_df = loans_payments_ndate_fixed_df.filter(
    (F.col("next_payment_date").rlike("^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\\d{4}$")) |
    F.col("next_payment_date").isNull()
)


# COMMAND ----------

valid_dates_df.printSchema()

# COMMAND ----------

valid_dates_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://silver-layer@loandatasa.dfs.core.windows.net/loan_repayment/") \
.save()
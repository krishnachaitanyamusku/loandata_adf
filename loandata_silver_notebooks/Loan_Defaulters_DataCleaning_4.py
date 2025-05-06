# Databricks notebook source
loans_def_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.option("inferSchema", True) \
.load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters.csv")

# COMMAND ----------

display(loans_def_raw_df)

# COMMAND ----------

loans_def_raw_df.printSchema()

# COMMAND ----------

loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

# COMMAND ----------

spark.sql("select distinct(delinq_2yrs) from loan_defaulters").display()

# COMMAND ----------

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").display()

# COMMAND ----------

loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"

# COMMAND ----------

loans_def_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loan_defaulters_schema) \
.load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters.csv")

# COMMAND ----------

loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

# COMMAND ----------

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").display()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

loans_def_processed_df = loans_def_raw_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

# COMMAND ----------

loans_def_processed_df.createOrReplaceTempView("loan_defaulters")

# COMMAND ----------

spark.sql("select count(*) from loan_defaulters where delinq_2yrs is null").display()

# COMMAND ----------

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").display()

# COMMAND ----------

loans_def_delinq_df = spark.sql("select member_id,delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) from loan_defaulters where delinq_2yrs > 0 or mths_since_last_delinq > 0")

# COMMAND ----------

display(loans_def_delinq_df)

# COMMAND ----------

loans_def_delinq_df.count()

# COMMAND ----------

loans_def_records_enq_df = spark.sql("select member_id from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")

# COMMAND ----------

display(loans_def_records_enq_df)

# COMMAND ----------

loans_def_records_enq_df.count()

# COMMAND ----------

loans_def_delinq_df.write \
.format("delta") \
.mode("overwrite") \
.option("path","abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters_deling_delta") \
.save()

# COMMAND ----------

loans_def_records_enq_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_defaulters_records_enq_delta") \
.save()

# COMMAND ----------

loans_def_p_pub_rec_df = loans_def_processed_df.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])

# COMMAND ----------

loans_def_processed_df.display()

# COMMAND ----------

loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])

# COMMAND ----------

loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])

# COMMAND ----------

loans_def_p_inq_last_6mths_df.display()

# COMMAND ----------

loans_def_p_inq_last_6mths_df.createOrReplaceTempView("loan_defaulters")

# COMMAND ----------

loans_def_detail_records_enq_df = spark.sql("select member_id, pub_rec, pub_rec_bankruptcies, inq_last_6mths from loan_defaulters")

# COMMAND ----------

loans_def_detail_records_enq_df.display()

# COMMAND ----------

loans_def_detail_records_enq_df.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_defaulters/loans_def_detail_records_enq_df_delta") \
.save()
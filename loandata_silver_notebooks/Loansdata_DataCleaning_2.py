# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. create a dataframe with proper datatypes and names

# COMMAND ----------

loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'

# COMMAND ----------

loans_raw_df = spark.read \
.format("csv") \
.option("header", True) \
.schema(loans_schema) \
.load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/loans_data/loan_data.csv")

# COMMAND ----------

display(loans_raw_df)

# COMMAND ----------

loans_raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. insert a new column named as ingestion date(current time)

# COMMAND ----------

loans_df_ingested = loans_raw_df.withColumn("ingest_date", current_timestamp())

# COMMAND ----------

display(loans_df_ingested)

# COMMAND ----------

from pyspark.sql.functions import col, when

# COMMAND ----------

loan_df_renamed = loans_df_ingested.withColumnRenamed("loan_amnt", "loan_amount") \
    .withColumnRenamed("funded_amnt", "funded_amount") \
    .withColumnRenamed("term", "loan_term_months") \
    .withColumnRenamed("int_rate", "interest_rate") \
    .withColumnRenamed("installment", "monthly_installment") \
    .withColumnRenamed("issue_d", "issue_date") \
    .withColumnRenamed("purpose", "loan_purpose") \
    .withColumnRenamed("title", "loan_title") \
    .withColumn(
        "loan_amount",
        when(col("loan_amount").rlike("^[0-9]+(\.[0-9]+)?$"), col("loan_amount").cast("float")).otherwise(None)
    ) \
    .withColumn(
        "funded_amount",
        when(col("funded_amount").rlike("^[0-9]+(\.[0-9]+)?$"), col("funded_amount").cast("float")).otherwise(None)
    ) \
    .withColumn(
        "interest_rate",
        when(col("interest_rate").rlike("^[0-9]+(\.[0-9]+)?$"), col("interest_rate").cast("float")).otherwise(None)
    ) \
    .withColumn(
        "monthly_installment",
        when(col("monthly_installment").rlike("^[0-9]+(\.[0-9]+)?$"), col("monthly_installment").cast("float")).otherwise(None)
    )

# COMMAND ----------

display(loan_df_renamed)

# COMMAND ----------

loan_df_renamed.createOrReplaceTempView("loans")

# COMMAND ----------

spark.sql("select count(*) from loans").show()

# COMMAND ----------

spark.sql("select * from loans where loan_amount is null").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Dropping the rows which has null values in the mentioned columns

# COMMAND ----------

columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]

# COMMAND ----------

loans_filtered_df = loans_df_ingested.na.drop(subset=columns_to_check)

# COMMAND ----------

loans_filtered_df.printSchema()

# COMMAND ----------

loans_filtered_df.count()

# COMMAND ----------

loans_filtered_df.createOrReplaceTempView("loans")

# COMMAND ----------

display(loans_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. convert loan_term_months to integer

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# COMMAND ----------

loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
.cast("int") / 12) \
.cast("int")) \
.withColumnRenamed("loan_term_months","loan_term_years")

# COMMAND ----------

display(loans_term_modified_df)

# COMMAND ----------

loans_term_modified_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Clean the loans_purpose column

# COMMAND ----------

loans_term_modified_df.createOrReplaceTempView("loans")

# COMMAND ----------

spark.sql("select distinct(loan_purpose) from loans")

# COMMAND ----------

spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

# COMMAND ----------

loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))

# COMMAND ----------

loans_purpose_modified.createOrReplaceTempView("loans")

# COMMAND ----------

spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())

# COMMAND ----------

loans_purpose_modified.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://silver-layer@loandatasa.dfs.core.windows.net/loans_data/") \
.save()
# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. create a dataframe with proper datatypes 

# COMMAND ----------

customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'

# COMMAND ----------

customers_raw_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .option("header", "true") \
    .load("abfss://bronze-layer@loandatasa.dfs.core.windows.net/customers_data/customers_data.csv")

# COMMAND ----------

display(customers_raw_df)

# COMMAND ----------

customers_raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename a few columns

# COMMAND ----------

customer_renamed_df = customers_raw_df.withColumnRenamed("annual_inc", "annual_income") \
.withColumnRenamed("addr_state", "address_state") \
.withColumnRenamed("zip_code", "address_zipcode") \
.withColumnRenamed("country", "address_country") \
.withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint", "joint_annual_income")

# COMMAND ----------

display(customer_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import col,when

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. insert a new column named as ingestion date(current time)

# COMMAND ----------

customers_df_ingested = customer_renamed_df.withColumn("ingest_date", current_timestamp())

# COMMAND ----------

display(customers_df_ingested)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Remove complete duplicate rows

# COMMAND ----------

customers_df_ingested.count()

# COMMAND ----------

customers_distinct = customers_df_ingested.distinct()

# COMMAND ----------

customers_distinct.printSchema()

# COMMAND ----------

customers_distinct.count()

# COMMAND ----------

customer_cleaned_df = customers_distinct.withColumn(
        "annual_income",
        when(
            col("annual_income").rlike("^[0-9]+(\.[0-9]+)?$"),
            col("annual_income").cast("float")
        ).otherwise(None)
    ) \
    .withColumn(
        "total_high_credit_limit",
        when(
            col("total_high_credit_limit").rlike("^[0-9]+(\.[0-9]+)?$"),
            col("total_high_credit_limit").cast("float")
        ).otherwise(None)
    ) \
    .withColumn(
        "joint_annual_income",
        when(
            col("joint_annual_income").rlike("^[0-9]+(\.[0-9]+)?$"),
            col("joint_annual_income").cast("float")
        ).otherwise(None)
    )

# COMMAND ----------

customer_cleaned_df.printSchema()

# COMMAND ----------

display(customer_cleaned_df)

# COMMAND ----------

customer_cleaned_df.createOrReplaceTempView("customers")

# COMMAND ----------

spark.sql("select * from customers").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove the rows where annual_income is null

# COMMAND ----------

spark.sql("select count(*) from customers where annual_income is null")

# COMMAND ----------

customers_income_filtered = spark.sql("select * from customers where annual_income is not null")

# COMMAND ----------

customers_income_filtered.createOrReplaceTempView("customers")

# COMMAND ----------

spark.sql("select count(*) from customers where annual_income is null")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. convert emp_length to integer

# COMMAND ----------

spark.sql("select distinct(emp_length) from customers")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# COMMAND ----------

customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)",""))

# COMMAND ----------

display(customers_emplength_cleaned)

# COMMAND ----------

customers_emplength_cleaned.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,expr

# COMMAND ----------

customers_emplength_casted = customers_emplength_cleaned.withColumn(
        "emp_length",
        when(
            col("emp_length").rlike("^[0-9]+$"),
            col("emp_length").cast("int")
        ).otherwise(None)
    )



# COMMAND ----------

display(customers_emplength_casted)

# COMMAND ----------

customers_emplength_casted.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. we need to replace all the nulls in emp_length column with average of this column

# COMMAND ----------

customers_emplength_casted.filter("emp_length is null").count()

# COMMAND ----------

customers_emplength_casted.createOrReplaceTempView("customers")

# COMMAND ----------

avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()

# COMMAND ----------

print(avg_emp_length)

# COMMAND ----------

avg_emp_duration = avg_emp_length[0][0]

# COMMAND ----------

print(avg_emp_duration)

# COMMAND ----------

customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])

# COMMAND ----------

display(customers_emplength_replaced)

# COMMAND ----------

customers_emplength_replaced.filter("emp_length is null").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Clean the address_state(it should be 2 characters only),replace all others with NA

# COMMAND ----------

customers_emplength_replaced.createOrReplaceTempView("customers")

# COMMAND ----------

spark.sql("select distinct(address_state) from customers")

# COMMAND ----------

spark.sql("select count(address_state) from customers where length(address_state)>2").show()

# COMMAND ----------

from pyspark.sql.functions import when, col, length

# COMMAND ----------

customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state"))
)

# COMMAND ----------

display(customers_state_cleaned)

# COMMAND ----------

customers_state_cleaned.select("address_state").distinct().show()

# COMMAND ----------

customers_state_cleaned.write \
.format("delta") \
.mode("overwrite") \
.option("path", "abfss://silver-layer@loandatasa.dfs.core.windows.net/customers_data/") \
.save()
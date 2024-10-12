# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, FloatType, DateType

#Ingestion Process
def IngestCSV(path):
    schema = StructType([
        StructField("FirstName", StringType()),
        StructField("LastName", StringType()),
        StructField("Company", StringType()),
        StructField("BirthDate", IntegerType()),
        StructField("Salary", FloatType()),
        StructField("Address", StringType()),
        StructField("Suburb", StringType()),
        StructField("State", StringType()),
        StructField("Post", IntegerType()),
        StructField("Phone", IntegerType()),
        StructField("Mobile", IntegerType()),
        StructField("Email", StringType()),
    ])    
    df = (spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .schema(schema)
    .load(path, format="csv", sep="|", mode="PERMISSIVE", header="false")
    )
    return df

# COMMAND ----------

#Transform Process
def Transform(df):
    df = df.withColumn("FullName", F.concat_ws(" ", F.trim(F.col("FirstName")), F.trim(F.col("LastName"))))
    df = df.withColumn('SalaryBucket',  F.when(F.col('Salary') < 50000, F.lit("A"))
                                         .when((F.col('Salary') >= 50000) & (F.col('Salary') <= 100000), F.lit("B"))
                                         .otherwise(F.lit("C")))
                       
    df = df.withColumn('Salary', F.concat(F.lit("$"), F.format_number('Salary', 4).cast("string")))
    df = df.withColumn('BirthDateTemp', F.from_unixtime('BirthDate').cast(DateType()))
    df = df.withColumn('BirthDate', F.date_format(F.from_unixtime('BirthDate').cast(DateType()), 'dd/MM/yyyy'))
    df = df.withColumn('age', (F.months_between(F.current_date(), F.col('BirthDateTemp')) / 12).cast('int'))
    df = df.drop("FirstName", "LastName", "BirthDateTemp")
    df.createOrReplaceTempView("member_data")       
    return df


# COMMAND ----------

#Load Process - Extract to JSON 
def Load(df):
    results = df.toPandas().to_json(orient='records')
    print(results)

# COMMAND ----------

#ETL Pipeline Run
Load(Transform(IngestCSV("abfss://test.dfs.core.windows.net/member-data.csv")))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from member_data

# COMMAND ----------



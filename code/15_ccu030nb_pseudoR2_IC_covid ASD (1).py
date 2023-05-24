# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists dars_nic_391419_j3w9t_collab.ccu030_temp; 
# MAGIC create table if not exists dars_nic_391419_j3w9t_collab.ccu030_temp as 
# MAGIC select *, 
# MAGIC case when covid_severity = "01_not_hospitalised" THEN 0 ELSE 1 END AS severe,
# MAGIC case when SEX = '2' THEN 1 
# MAGIC when SEX = '1' THEN 0
# MAGIC ELSE null END AS female,
# MAGIC case when ethnic_group = 'Asian or Asian British' then 1
# MAGIC when ethnic_group is null then null
# MAGIC ELSE 0 END AS asian,
# MAGIC case when ethnic_group = 'Black or Black British' then 1
# MAGIC when ethnic_group is null then null
# MAGIC ELSE 0 END AS black,
# MAGIC case when ethnic_group = 'Mixed' then 1
# MAGIC when ethnic_group is null then null
# MAGIC ELSE 0 END AS mixed,
# MAGIC case when ethnic_group = 'Other Ethnic Groups' then 1
# MAGIC when ethnic_group is null then null
# MAGIC ELSE 0 END AS other_ethnicity
# MAGIC from dars_nic_391419_j3w9t_collab.ccu030_20220812_2133_patient_skinny_record_enhanced19
# MAGIC where age_at_covid >= 18 and age_at_covid <= 100

# COMMAND ----------

# Scikit-Learn ?0.20 is required
import sklearn
assert sklearn.__version__ >= "0.20"

# Common imports
import numpy as np
import os
import pandas as pd

np.random.seed(42)

import pyarrow as pa
import pyarrow.parquet as pq

# COMMAND ----------

# MAGIC %md convert the table to a spark df:

# COMMAND ----------

import pyspark.sql.functions as f
df = spark.table(f'dars_nic_391419_j3w9t_collab.ccu030_temp')

# COMMAND ----------

# replace nulls with zeros on some columns
df = df.fillna(value=0,subset=["id", "autism",  "medcount", "N_DOSES", "niv", "imv", "ecmo", "ALC", "AB", "ANX", "AST", "ATR", "BLI", "BRO", "CAN", "CHD", "CKD", "CLD", "COPD", "DEM", "DEP", "DIA", "DIV", "EPI", "HF", "HL", "HYP", "IBD", "IBS", "MIG", "MS", "PUD", "PRK", "PSD", "PSM", "PSO", "PVD", "RHE", "SCZ", "SIN", "STR", "THY", "astrazeneca", "pfizer", "moderna", "other_vaccine"])

# COMMAND ----------

# create the ltc_count variable 
# it looks like I need to create a new df, it can't just overwrite?
from pyspark.sql.functions import col
df = df.withColumn("ltc_count", col("ALC")+col("AB")+col("ANX")+col("AST")+col("ATR")+col("BLI")+col("BRO")+col("CAN")+col("CHD")+col("CKD")+col("CLD")+col("COPD")+col("DEM")+col("DEP")+col("DIA")+col("DIV")+col("EPI")+col("HF")+col("HL")+col("HYP")+col("IBD")+col("IBS")+col("MIG")+col("MS")+col("PUD")+col("PRK")+col("PSD")+col("PSM")+col("PSO")+col("PVD")+col("RHE")+col("SCZ")+col("SIN")+col("STR")+col("THY"))

# COMMAND ----------

# top code ltc_count to aoid perfect collinearity
from pyspark.sql.functions import when
df = df.withColumn("ltc_count", when(df["ltc_count"] > 25, 25).otherwise(df["ltc_count"]))
df = df.withColumn("medcount", when(df["medcount"] > 20, 20).otherwise(df["medcount"]))

# COMMAND ----------

# df.head()

# COMMAND ----------

# the example in the book is a pandas frame, so I'm converting spark df to pandas df
# spark.conf.set("spark.sql.execution.arrow.enabled", "false")

# COMMAND ----------

# select columns - it was throwing a 'out of memory' error otherwise
myvars = ["id","autism",  "female","severe","N_DOSES","medcount","ALC","BRO","CAN","CHD","CKD","CLD","COPD","DIA","EPI","HF","HL","HYP","MS","PSO","PVD","STR","THY","age_at_covid","deci_imd","asian","black","mixed","other_ethnicity","ltc_count", "astrazeneca", "pfizer", "moderna", "other_vaccine"]
# dropped: "AST","ATR","BLI", "SIN","SCZ", "RHE","DEM","DEP","DIV","IBD","IBS","MIG","PUD","PRK","PSD","PSM",

df = df.select(myvars)

# compress the dataset
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import ByteType

for var in myvars:
  df = df.withColumn(f'{var}', f.col(f'{var}').cast(ByteType()))

# COMMAND ----------

df.dtypes

# COMMAND ----------

# from pyspark.sql.types import DecimalType
# df = df.withColumn("age_at_covid",col("age_at_covid").cast(DecimalType(precision=5, scale=2)))
# precision = total number of digits, scale = number of decimals. 5,2 means from -999.99 to 999.99

# COMMAND ----------

# df.head()

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

# force all variables to be numeric
pdf = pdf.apply(pd.to_numeric, errors='coerce')

# COMMAND ----------

pdf.head()

# COMMAND ----------

# check data types
pdf.dtypes

# COMMAND ----------

pdf.head()

# COMMAND ----------

pdf.info()

# COMMAND ----------

# save the full dataset as X and y
# but first drop NAs
pdf = pdf.dropna()
pdf_GPop = pdf[(pdf["autism"] == 0) | ((pdf["id"] == 1) & (pdf["autism"] == 1))]
X_GPop = pdf_GPop.drop("severe", axis=1)
y_GPop = pdf_GPop["severe"].copy()

# also for autism no ID records only
pdf_id = pdf[(pdf["id"] == 0) & (pdf["autism"] == 1)]
X_id = pdf_id.drop("severe", axis=1)
y_id = pdf_id["severe"].copy()

# COMMAND ----------

X_id.head()

# COMMAND ----------

# MAGIC %md **2. Modelling:**

# COMMAND ----------

import statsmodels.api as sm

# COMMAND ----------

# MAGIC %md **non-ID:**

# COMMAND ----------

# intercept-only
X_try = np.ones((X_GPop.shape[0],1))
mymodel = sm.Logit(y_GPop, sm.add_constant(X_try)).fit(disp=0)
print(mymodel.summary())
print("Pseudo R2:", round(mymodel.prsquared, 3))
print("AIC:", round(mymodel.aic,1))
print("BIC:", round(mymodel.bic,1))

# COMMAND ----------

varlist = ['age_at_covid', 'female', 'asian', 'black', 'mixed', 'other_ethnicity', 'deci_imd', 'N_DOSES', 'astrazeneca', 'pfizer', 'moderna', 'other_vaccine', 'ltc_count', 'medcount' ]

# create two empty matrices, one for results and one for rownames
mat = np.empty((len(varlist),3))
rownames = np.empty([len(varlist),1], dtype="U20")
# U20 stands for unicode string of max length 20. Numpy matrix can only take one data type, so I need two matrices, one with rownames and one with numbers.
# do not use S20 etc because it will print the letter 'b' at the beginning of the string, for some reason.

templist = []
i = -1
for var in varlist:
  templist.append(var)
  i = i + 1
  print("Loop ", i+1, "of ", len(varlist))
#   create a df with selected predictors:
  X_GPop_temp = X_GPop[templist]
  mymodel = sm.Logit(y_GPop, sm.add_constant(X_GPop_temp)).fit(disp=0)
#   print("ID, Predictors: ", templist)
#   print("Pseudo R2:", round(mymodel.prsquared, 3))
#   print("AIC:", round(mymodel.aic,1))
#   print("BIC:", round(mymodel.bic,1), "\n")
  rownames[[i],0] = var
  mat[[i],0] = round(mymodel.prsquared, 3)
  mat[[i],1] = round(mymodel.aic,1)
  mat[[i],2] = round(mymodel.bic,1)
# print(rownames)
# print(mat)

# create a column that will be used for preserving order
order = np.arange(0,len(varlist))
# need it to be vertical rather than horizontal:
order = np.vstack(order)
# order
# join the two matrices and the ordering column
finalmat = np.hstack((order, rownames, mat))
# print(finalmat)
# in order to save it as a table you need to first convert to pandas, then to spark
mypdf = pd.DataFrame(finalmat)
mypdf.rename(columns={0: "orig_order", 1: "", 2: "Pseudo_R2", 3: "AIC", 4: "BIC"}, inplace=True)
# 'inplace = TRUE' means that you want to overwrite the df
# mypdf
df = spark.createDataFrame(mypdf)
# df.show()
df.write.mode("overwrite").saveAsTable("dars_nic_391419_j3w9t_collab.ccu030_temp")

# COMMAND ----------

dir(sm)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu030_temp
# MAGIC order by orig_order asc
# MAGIC -- this can be manually exported to csv by using the buttons below the data

# COMMAND ----------

# MAGIC %md **now ID only:**

# COMMAND ----------

# intercept-only
X_try = np.ones((X_id.shape[0],1))
mymodel = sm.Logit(y_id, sm.add_constant(X_try)).fit(disp=0)
print(mymodel.summary())
print("Pseudo R2:", round(mymodel.prsquared, 3))
print("AIC:", round(mymodel.aic,1))
print("BIC:", round(mymodel.bic,1))

# COMMAND ----------

varlist = ['age_at_covid', 'female', 'asian', 'black', 'mixed', 'other_ethnicity', 'deci_imd', 'N_DOSES', 'astrazeneca', 'pfizer', 'moderna', 'other_vaccine', 'ltc_count', 'medcount' ]

# create two empty matrices, one for results and one for rownames
mat = np.empty((len(varlist),3))
rownames = np.empty([len(varlist),1], dtype="U20")
# U20 stands for unicode string of max length 20. Numpy matrix can only take one data type, so I need two matrices, one with rownames and one with numbers.
# do not use S20 etc because it will print the letter 'b' at the beginning of the string, for some reason.

templist = []
i = -1
for var in varlist:
  templist.append(var)
  i = i + 1
#   create a df with selected predictors:
  X_id_temp = X_id[templist]
  mymodel = sm.Logit(y_id, sm.add_constant(X_id_temp)).fit(disp=0, method='bfgs')
#   note that I used method='bfgs' because it was not converging otherwise for autism no ID
#   print("ID, Predictors: ", templist)
#   print("Pseudo R2:", round(mymodel.prsquared, 3))
#   print("AIC:", round(mymodel.aic,1))
#   print("BIC:", round(mymodel.bic,1), "\n")
  rownames[[i],0] = var
  mat[[i],0] = round(mymodel.prsquared, 3)
  mat[[i],1] = round(mymodel.aic,1)
  mat[[i],2] = round(mymodel.bic,1)
# print(rownames)
# print(mat)

# create a column that will be used for preserving order
order = np.arange(0,len(varlist))
# need it to be vertical rather than horizontal:
order = np.vstack(order)
# order
# join the two matrices and the ordering column
finalmat = np.hstack((order, rownames, mat))
# print(finalmat)
# in order to save it as a table you need to first convert to pandas, then to spark
mypdf = pd.DataFrame(finalmat)
mypdf.rename(columns={0: "orig_order", 1: "", 2: "Pseudo_R2", 3: "AIC", 4: "BIC"}, inplace=True)
# 'inplace = TRUE' means that you want to overwrite the df
# mypdf
df = spark.createDataFrame(mypdf)
# df.show()
df.write.mode("overwrite").saveAsTable("dars_nic_391419_j3w9t_collab.ccu030_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dars_nic_391419_j3w9t_collab.ccu030_temp
# MAGIC order by orig_order asc
# MAGIC -- this can be manually exported to csv by using the buttons below the data

# COMMAND ----------

# list attributes
print(dir(mymodel))

# COMMAND ----------



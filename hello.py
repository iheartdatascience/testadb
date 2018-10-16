# Databricks notebook source
#this is just a test for comments
%run "./Includes/Classroom-Setup"    

# COMMAND ----------

path = "/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv"

logDF = spark.read.option("header", True).csv(path).sample(withReplacement=False, fraction=0.3, seed=3)

display(logDF)

# COMMAND ----------

from pyspark.sql.functions import col
serverErrorDF = (logDF
   .filter((col("code") >= 500) & (col("code") < 600))
   .select("date", "time", "extention", "code")
) 

# COMMAND ----------

display(serverErrorDF)
serverErrorDF.count()

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, hour, col

countsDF = (serverErrorDF
   .select(hour(from_utc_timestamp(col("time"), "GMT")).alias("hour"))
   .groupBy("hour")
   .count()
   .orderBy("hour"))

display(countsDF)

# COMMAND ----------

(serverErrorDF
   .write
   .mode("overwrite")
   .parquet("/tmp" + username + "/log20170329/serverErrorDF.parquet")
)

# COMMAND ----------

display(logDF)

# COMMAND ----------

from pyspark.sql.functions import desc

ipCountDF = (logDF
   .select("ip")
   .groupBy("ip")
   .count()
   .orderBy(desc("count"))
)

# COMMAND ----------

ip1, count1 = ipCountDF.first()
cols = set(ipCountDF.columns)

dbTest("ET1-P-02-01-01", "213.152.28.bhe", ip1)
dbTest("ET1-P-02-01-02", True, count1 > 500000 and count1 < 550000)
dbTest("ET1-P-02-01-03", {'count', 'ip'}, cols)

print("Tests passed!")

# COMMAND ----------

writePath = "/tmp/" + username + "/ipCount.parquet"

(ipCountDF
   .write
   .mode("overwrite")
   .parquet(writePath)
)

# COMMAND ----------

dbutils.fs.ls("/tmp/" + username + "/ipCount.parquet")
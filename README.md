# PySpark Operations On Amazon Tweets
This repository shows the implementation of Spark context, Spark SQL context on Amazon Tweets data set with 400k Tweets. I dealt with tweet_id (id_str), Tweet_created_time, Retweet_count, Favourite_count to find the days with high influx of tweets. 
Analyzed the tweets on the busiest day to find the words that were repeated the most in the selected tweets.

Summary of operations:

**Import csv file:**
data = spark.read.format("csv").option("header","true").load("filepath/filename.csv")

**Selecting the required columns from a data frame:**
data.select("column_name1",'colname2','colname3','colname4')

**Printing data types of columns:**
types = [f.dataType for f in data1.schema.fields]
types

**Printing distinct values of a column:**
data.select("column_name").distinct().show()

**Parsing a column to create additional columns:**
from pyspark.sql.functions import split
tweet_created_at is in the format: "Tue Nov 01 02:39:55 +0000 2016"
a = split(dat_filtered["tweet_created_at"], ' ')
dat_filtered = dat_filtered.withColumn('Month', a.getItem(1))
dat_filtered = dat_filtered1.withColumn('Date', a.getItem(2))
dat_filtered = dat_filtered1.withColumn('Year', a.getItem(5))

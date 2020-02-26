# PySpark Operations On Amazon Tweets  
This repository shows the implementation of Spark context, Spark SQL context on Amazon Tweets data set with 400k Tweets. I dealt with tweet_id (id_str), Tweet_created_time, Retweet_count, Favourite_count to find the days with high influx of tweets.  
Analyzed the tweets on the busiest day to find the words that were repeated the most in the selected tweets.

Summary of operations:  

**1. Import csv file:**  
data = spark.read.format("csv").option("header","true").load("filepath/filename.csv")  

**2. Selecting the required columns from a data frame:**  
data.select("column_name1",'colname2','colname3','colname4')  

**3. Printing data types of columns:**  
types = [f.dataType for f in data1.schema.fields]  
types  

**4. Printing distinct values of a column:**  
data.select("column_name").distinct().show()  

**5. Parsing a column to create additional columns:**  
from pyspark.sql.functions import split  
tweet_created_at is in the format: "Tue Nov 01 02:39:55 +0000 2016"  
a = split(dat_filtered["tweet_created_at"], ' ')  
dat_filtered = dat_filtered.withColumn('Month', a.getItem(1))  
dat_filtered = dat_filtered1.withColumn('Date', a.getItem(2))  
dat_filtered = dat_filtered1.withColumn('Year', a.getItem(5))  

**6. Concatenating multiple columns to form a new one**  
dat_filtered.select(concat(col("Month"), lit(" "), col("Date"),lit(" "), col("Year")).alias("Date"))  

**7. Importing SQL funtions col,lit**  
import pyspark.sql.functions as sq 
dat_filtered.withColumn("tweet_created_at",sq.concat(col("Month"), sq.lit(" "), sq.col("Date"),sq.lit(" "), sq.col("Year")))

**8. Aggregations on dataframe**  
df.groupby(df.date).agg(sq.count('id_str').alias("count_of_tweets"))  
Above command counts the number of tweets grouped by the date  

**9. Initializing spark context and sql context to perform SQL queries**  
conf = pyspark.SparkConf()  
sc = pyspark.SparkContext.getOrCreate(conf=conf)  
from pyspark.sql import SQLContext  
sqlcontext = SQLContext(sc)  
counts.registerTempTable("tmpcounts")  
counts_ordered = sqlcontext.sql("SELECT * FROM tmpcounts order by count_of_tweets desc limit 5")  

"""
Advance Data Science on Ramp 
Intro to Spark - Assignment 4 Part1
Jainendra Kumar | jaikumar@iu.edu

"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql import *

conf = SparkConf().setMaster("local").setAppName("Assignment 4")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("Python Spark SQL") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

sc.setLogLevel("ERROR")

# Using Spark Transformation and Actions
print("------------PART 1:------SPARK TRANSFORMATION AND ACTIONS----------------------------")
print("DATA LOADED")
df = spark.read.load('Fake_data.csv',format="csv", sep=",", inferSchema="true", header="true")

# Question 1
# Find birth country which has highest amount of people
print("Answer 1. Birth country with the highest amount of people:")
df.groupby('Birth_Country').count().orderBy(func.desc('count')).limit(1).show()

# Question 2
# Find average income of people who are born in united states of america
print("Answer 2. Average income of people who are born in united states of america:")
df.groupby('Birth_Country').agg({'Income':'mean'}).filter(df.Birth_Country=='United States of America').show()

# Question 3
# How many people has income over 100,000 but their loan is not approved.
print("Answer 3. People having income over 100,000 but their loan is not approved:")
output = df.filter('Income > 100000 AND Loan_Approved = "FALSE"').count()
print(output)

# Question 4
# Find top 10 people with highest income in United States of america. (Print their names, income and jobs)
print("Answer 4. Top 10 people with highest income in USA:")
df.filter('Birth_Country = "United States of America"').orderBy(func.desc('Income')).select('First_Name','Last_name','Income','Job','Birth_Country').limit(10).show()

# Question 5
# How many number of distinct jobs are there?
print("Answer 5. Number of distinct jobs:")
jobs = df.select('Job').distinct().count()
print(jobs)

# Question 6
# How many writers earn less than 100,000?
print("Answer 6. Writers that earn less than 100,000:")
writers = df.filter(df.Job=='Writer').filter(df.Income<100000).count()
print(writers)


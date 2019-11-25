"""
Advance Data Science on Ramp 
Intro to Spark - Assignment 4 Part2
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

df = spark.read.load('Fake_data.csv',format="csv", sep=",", inferSchema="true", header="true")

# Using SparkSQL
print("----------------------PART 2:------SPARK SQL-------------------------------------")
df.createOrReplaceTempView("people")

# Question 1
print("Answer 1. Birth country with the highest amount of people:")
spark.sql('SELECT Birth_Country, COUNT(SSN) AS Count FROM people GROUP BY Birth_Country HAVING COUNT(SSN)=(SELECT MAX(TotalSSN) FROM (SELECT COUNT(SSN) AS TotalSSN FROM people GROUP BY Birth_Country))').show()

# Question 2
print("Answer 2. Average income of people who are born in united states of america:")
spark.sql('SELECT Birth_Country, AVG(Income) FROM people GROUP BY Birth_Country HAVING Birth_Country="United States of America"').show()

# Question 3
print("Answer 3. People having income over 100,000 but their loan is not approved:")
spark.sql('SELECT COUNT(SSN) AS No_of_people FROM people WHERE Loan_Approved="FALSE" AND Income>100000').show()

# Question 4
print("Answer 4. Top 10 people with highest income in USA:")
spark.sql('SELECT First_Name, Last_name, Income, Job, Birth_Country FROM people WHERE Birth_Country="United States of America" ORDER BY Income DESC LIMIT 10').show()

# Question 5
print("Answer 5. Number of distinct jobs:")
spark.sql('SELECT COUNT(DISTINCT Job) AS TotalCount FROM people').show()

# Question 6
print("Answer 6. Writers that earn less than 100,000:")
spark.sql('SELECT COUNT(Job) AS Writers FROM people WHERE Job="Writer" AND Income<100000').show()

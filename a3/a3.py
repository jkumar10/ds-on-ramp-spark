from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from io import StringIO
import csv,json

conf = SparkConf().setMaster("local").setAppName("Assignment 3")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Answer 1
# Importing data using JSON library
print("---------ANSWER 1--IMPORTING DATA USING JSON------------------")
inp_file = sc.textFile('/opt/spark/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json')
data_json = inp_file.map(lambda x: json.load(x))
print("----------------IMPORT COMPLETED------------------------------")

# Question 2
# Importing data using SQL Context
print("--------ANSWER 2--IMPORTING DATA USING SQL CONTEXT------------")
sql_ctext = SQLContext(sc)
sql_df = sql_ctext.read.json('/opt/spark/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json')
sql_df.printSchema()
sql_df.registerTempTable("people")
distinct_names = sql_ctext.sql("SELECT DISTINCT name FROM people")
print("---------------IMPORT USING SQL CONTEXT COMPLETED-------------")

print("Distinct names in people.json:")
distinct_names.show()

# Question 3:
# Importing people.txt into a RDD using a built in CSV library
print("---------ANSWER 3--IMPORTING CSV-------------------------")
def load_record(line):
    # Parsing CSV
    input = StringIO(line)
    reader = csv.DictReader(input, fieldnames=["name", "age"])
    return next(reader)

input = sc.textFile('/opt/spark/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.txt').map(load_record)
print(input.collect())

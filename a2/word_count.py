from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("First Spark App")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

def filtertext(text):
	return re.sub(r'[^a-zA-Z0-9\s]', '', text).lower().strip()

read1 = sc.textFile('assignment_2_datafile.txt').map(filtertext)
read1.saveAsTextFile('FilteredData')
splitted_text = read1.flatMap(lambda line: line.split(" "))
filtered_words = splitted_text.filter(lambda x: len(x)>=3 and x!=' ')
output = filtered_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
output.saveAsTextFile('Output')


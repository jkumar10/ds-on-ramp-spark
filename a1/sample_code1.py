
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("First Spark App")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")
text_file=sc.textFile("/home/ubuntu/spark/sampletext.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts_collect=counts.collect()
counts.saveAsTextFile("/home/ubuntu/spark/output1")
dict_counts = {}
for word, val in counts_collect:
	dict_counts[word] = val

top_10 = sorted(dict_counts.items(), key=lambda x: x[1])
print(top_10)


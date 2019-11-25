from pyspark import SparkConf, SparkContext
import random
conf = SparkConf().setMaster("local").setAppName("First Spark App")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

NUM_SAMPLES=10000
count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))

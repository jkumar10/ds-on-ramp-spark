from pyspark import SparkConf, SparkContext
from functools import partial
conf = SparkConf().setMaster("local").setAppName("Broadcast and Accumulator")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Accumulator variable 
val = sc.accumulator(0)
def add(num):
    global val
    val+=num 
partitions = sc.parallelize([20,50,60,70])
partitions.foreach(add)
result = val.value
print("\n Using accumulator")
print("Sum of all values", partitions.collect(),"=", result)

# Broadcast variable 
partitions = sc.parallelize([2,4,6,8])
pi = sc.broadcast(3.1416)
def volume(r):
    p=pi.value
    v=(4/3.0)*p*r*r*r
    return int(v)
result = partitions.map(volume)
print("\n Using broadcast variable")
print("Radius of spehere =", partitions.collect())
print("Volume of circle =", result.collect())

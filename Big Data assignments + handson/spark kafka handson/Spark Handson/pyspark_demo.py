# -*- coding: utf-8 -*-
"""PySpark_demo.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1SrQYPGIB2ABmxPGgnX9iABwsn6H4A2j_

# New Section
"""

import os, sys
sys.executable

pip install pyspark

os.environ["PYSPARK_PYTHON"]=sys.executable
#os.environ["SPARK_HOME"]="/opt/spark-3.0.1-bin-hadoop2.7"

import pyspark
sc=pyspark.SparkContext(master="local", appName="Demo")
sc.setLogLevel("ERROR")

print(sc)

"""# Create RDD"""

data=list(range(8))
rdd=sc.parallelize(data)  #create collection
rdd

rdd.collect()

"""# Map"""

rdd=sc.parallelize(list(range(8)))
rdd.map(lambda x:x*x).collect()

"""# Filter"""

rdd.filter(lambda x:x%2==1).collect()

rdd=sc.parallelize([1,2,3])
rdd.flatMap(lambda x:[x,x*x,x*10]).collect()

# sc.parallelize([1,2,3,4,5,6],3).glom().collect()
sc.parallelize([1,2,3,4,5,6,7,8,9,10,11],8).coalesce(3).glom().collect()

"""# Group By"""

rdd=sc.parallelize(["John","Fred","Anna","James"])
rdd=rdd.groupBy(lambda w:w[0])
[(k,list(v)) for (k,v) in rdd.collect()]
# rdd.collect()

"""# GroupByKey"""

rdd=sc.parallelize([("B",5),("B",4),("A",3),("A",6)])
rdd=rdd.groupByKey()
[(j[0],list(j[1])) for j in rdd.collect()]
# rdd.collect()

"""# Join"""

x=sc.parallelize([("a",1),("b",2)])
y=sc.parallelize([("a",3),("b",2),("b",5)])
x.join(y).collect()

"""# Distinct"""

rdd=sc.parallelize([1,2,3,1,2,3,4,5,2])
rdd.distinct().collect()

"""# Key By"""

rdd=sc.parallelize(["John","Fred","Anna","James"])
rdd.keyBy(lambda w:w[0]).collect()
# rdd.collect()

"""# Reduce"""

from operator import add
rdd=sc.parallelize(list(range(8)))
rdd.map(lambda x:x*x).reduce(add)

rdd=sc.parallelize([1,2,3,4])
print(rdd.min())
print(rdd.max())
print(rdd.mean())
print(rdd.variance())
print(rdd.stdev())

"""# CountByKey"""

rdd=sc.parallelize([("J","James"),("F","Fred"),("J","John"),("A","Anna")])
rdd.countByKey()

"""# Top"""

print(sc.parallelize([1,2,3,4,5]).top(3))

"""# Word Count"""

from pyspark.sql import SparkSession
spark = SparkSession.builder\
                            .master("local")\
                            .appName("WordCount")\
                            .getOrCreate()
sc=spark.sparkContext
text_file=sc.textFile("Commands.txt")
counts=text_file.flatMap(lambda line: line.split(" ")) \
                          .map(lambda word: (word,1))  \
                          .reduceByKey(lambda x,y: x+y)
output=counts.collect()
for (word,count) in output:
  print("%s: %i" % (word, count))
sc.stop()
spark.stop()


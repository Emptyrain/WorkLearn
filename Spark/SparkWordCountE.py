#-# -*- coding = utf-8 -*-
# @time:2021/6/28 15:48
# Author:Rain
# @File:SparkWordCountE.py
# @Software:PyCharm

import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext('local[*]','WordCount')
Data = sc.textFile('哈利波特.txt')
RDD = Data.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)\
    .sortBy(lambda x:x[1],ascending=False).take(10)
print(RDD)



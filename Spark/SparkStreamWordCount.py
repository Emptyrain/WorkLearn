#-# -*- coding = utf-8 -*-
# @time:2021/6/28 16:58
# Author:Rain
# @File:SparkStreamWordCount.py
# @Software:PyCharm

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc =  SparkContext('local[*]','StreamWordCount')
sc.setLogLevel('ERROR')
#5秒一个批次
scs = StreamingContext(sc,5)
#每次处理数据后进行本地储存
scs.checkpoint("./checkPoint_1")
#链接数据传入端口
lines = scs.socketTextStream('localhost', 9999)

#每10秒计算一次最近20秒内传入的数据
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).window(20,10)

words.pprint()
#开始计算
scs.start()
#等待计算结束
scs.awaitTermination()
#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

N=10
lines = lines.map(lambda line : line.split(': '))
LOGGER.info(f'{str(lines.take(N))}>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")

sc.stop()


#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stop_words = []
with open(stopWordsPath) as f:
    stop_words = []
    with open(stopWordsPath) as f:
        stop_words = [line.strip() for line in f]

delimiters = None
with open(delimitersPath) as f:
    delimiters = f.readline()
    delimiters = list(delimiters)

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO
print(lines)

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()

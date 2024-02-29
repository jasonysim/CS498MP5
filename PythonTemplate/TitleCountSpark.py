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

def tokenize_words(line):
    words = line.lower().encode("utf-8")
    for delimiter in delimiters:
        words = ' '.join(words.split(delimiter))
    words = [word for word in words.split() if word not in stop_words]
    return words

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO: 
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

def tokenize_words(line): 
    return line.lower().split() 
lines = lines.flatMap(tokenize_words)
lines = lines.map(lambda x: (x, 1))
lines = lines.reduceByKey(lambda x, y: x + y)
lines = lines.sortBy(lambda x: (-x[1], x[0]))

outputFile = open(sys.argv[4],"w")

#TODO1
#write results to output file. Format for each line: (line +"\n")
# LOGGER.info(f'{str(type(lines))}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
# def write_to_file(line):
#     LOGGER.info(f'{str(lines)}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
#     # outputFile.write(str(line) + "\n")

for line in lines.take(10):
    outputFile.write(str(line) + "\n")

sc.stop()


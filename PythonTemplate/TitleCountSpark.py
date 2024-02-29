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
    words = line.lower()
    for delimiter in delimiters:
        words = ' '.join(words.split(delimiter))
    words = [word for word in words.split() if word not in stop_words]
    return words

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO: 
N = 5
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

lines = lines.flatMap(tokenize_words)
lines = lines.map(lambda word: (word, 1))
lines = lines.reduceByKey(lambda word, c: word + c)
lines = lines.sortBy(lambda wordcount: wordcount[1], ascending=False)
top_lines = lines.take(N)
top_lines.sort(key=lambda x: x[0])

outputFile = open(sys.argv[4],"w")
for line in top_lines:
    outputFile.write(f'{line[0]}\t{line[1]}\n')

sc.stop()
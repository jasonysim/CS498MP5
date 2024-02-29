#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

# stopWordsPath = sys.argv[1]
# delimitersPath = sys.argv[2]
import os
stopWordsPath = os.path.join('C:\\Users\\Jason\\OneDrive\\School\\UIUC\\2024 Spring\\CS498\\CS498MP5\\CS498MP5\\PythonTemplate\\stopwords.txt')
delimitersPath = os.path.join('C:\\Users\\Jason\\OneDrive\\School\\UIUC\\2024 Spring\\CS498\\CS498MP5\\CS498MP5\\PythonTemplate\\delimiters.txt')

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

path = os.path.join('C:\\Users\\Jason\\OneDrive\\School\\UIUC\\2024 Spring\\CS498\\CS498MP5\\CS498MP5\\PythonTemplate\\dataset\\titles\\titles-test')
lines = sc.textFile(path, 1)

#TODO: 
lines = lines.flatMap(tokenize_words)
lines = lines.map(lambda x: (x, 1))
lines = lines.reduceByKey(lambda x, y: x + y)
lines = lines.sortBy(lambda x: x[1], ascending=False)
print(lines)

# outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()

#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

N = 10
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

def id_counter(line):
    results = []
    page_id, incoming_links = line.split(': ')
    incoming_links = incoming_links.split(' ')
    results.append((page_id,0))
    results.extend([(link,1) for link in incoming_links])
    return results

lines = lines.flatMap(id_counter)
lines = lines.reduceByKey(lambda x, y : x+y)
lines = lines.sortBy(lambda count: count[1], ascending=False)
top_lines = lines.take(N)
top_lines.sort(key=lambda x: x[0])

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for line in top_lines:
    output.write(f'{line[0]}\t{line[1]}\n')

sc.stop()


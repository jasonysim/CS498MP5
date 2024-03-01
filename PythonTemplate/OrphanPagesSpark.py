#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

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
lines = lines.filter(lambda x : x[1] == 0)
lines = lines.sortBy(lambda x : x[0])

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for lines in lines.collect():
    output.write(f'{lines[0]}\t{lines[1]}\n')

output.close()
sc.stop()


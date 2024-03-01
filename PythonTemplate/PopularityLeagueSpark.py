#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
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

leagueIds = sc.textFile(sys.argv[2], 1)

#TODO
league = leagueIds.collect()
lines = lines.filter(lambda x : x[0] in league)
lines = lines.map(lambda x : (x[1], set([x[0]])))
lines = lines.reduceByKey(lambda x, y : x.union(y))
lines = lines.sortBy(lambda x : x[0], ascending=True)

counter = 0
link_levels = []
for line in lines.collect():
    count, links = line
    for link in links:
        link_levels.append((link, counter))
    counter += len(links)

link_levels = sorted(link_levels, key=lambda x : x[0])
LOGGER.info(f'{str(link_levels)}>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
output = open(sys.argv[3], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for line in link_levels:
    output.write(f'{line[0]}\t{line[1]}\n')


sc.stop()


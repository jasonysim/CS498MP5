#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

N = 5
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

def parse_text(line):
    return line.split('\t')
lines = lines.flatMap(parse_text)

LOGGER.info(f'{str(lines.take(N))}>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

# elements of line will include (word, count) tuples
# caclulate mean, sum, min, max, variance

# ans1 = sum_total//N
ans2 = sum_total
# ans3 = lines.map(lambda x: int(x[1])).min()
# ans4 = lines.map(lambda x: int(x[1])).max()
# ans5 = lines.map(lambda x: int(x[1])).variance()

outputFile = open(sys.argv[2], "w")
'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''
# outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
# outputFile.write('Min\t%s\n' % ans3)
# outputFile.write('Max\t%s\n' % ans4)
# outputFile.write('Var\t%s\n' % ans5)

sc.stop()


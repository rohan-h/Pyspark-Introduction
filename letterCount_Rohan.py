import re
import sys
import string
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local")
sc = SparkContext(conf=conf)
lines = sc.textFile("pg100.txt")
#words = lines.flatMap(lambda l: re.findall(r'[A-Za-z]+', l))

## MODIFIED Flatmap function to ignore anything other than alphabets and cases.
## Added Findall to avoid empty regex expression splits
words = lines.flatMap(lambda l: re.findall(r'[A-Za-z]+|[\\W]+[A-Za-z]*', l))

##[w[0] to group by alphabets]
pairs = words.map(lambda w: (w[0].lower() if w else w, 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
counts.saveAsTextFile("/home/rohan/results1")
sc.stop()

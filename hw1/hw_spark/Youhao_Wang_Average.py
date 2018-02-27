from __future__ import print_function
from __future__ import division

import sys
import string
import re
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: Youhao_Wang_Average.py <Input-file> <Output-dir>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName = "inf553")

    input = sc.textFile(sys.argv[1])\
        .map(lambda line: line.encode("ascii", "ignore").split(","))\
        .map(lambda line: (line[3],line[18]))

    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    temp = input.map(lambda s: (s[0].replace("'","").replace("-","").translate(replace_punctuation).replace("( )+", " ").lower().strip() ,s[1]))
    pairs = temp.map(lambda s: (re.sub(' +',' ',s[0]), s[1]))
    pairs = pairs.filter(lambda x: x[0] is not None).filter(lambda x: x[0] != "").filter(lambda x: x[0] != "event")

    tempRes = pairs.groupByKey().map(lambda x: (x[0], list(map(int, x[1]))))
    res = tempRes.map(lambda x: (x[0], len(x[1]), sum(x[1]) / len(x[1]) ))
    res = res.sortBy(lambda x: x[0], True).collect()

   # print(pairs.collect())

    text_file = open(sys.argv[2], "w")
    for x in res:
        text_file.write(x[0] + "\t" + str(x[1]) + "\t" + str(round(x[2], 3)) + "\n") 
    #for i in input:
    #    text_file.write(i)

import sys
import string
import itertools
from pyspark import SparkContext
import collections

if __name__ == "__main__":
    """ 
    if len(sys.argv) != 4:
        print("Usage: Youhao_Wang_SON.py baskets.txt <support> output.txt", file=sys.stderr)
        exit(-1)
    """
    sc = SparkContext(appName = "inf553")
    support = float(sys.argv[2])

    def stage1(lines):
        data = list(lines)
        s = support * len(data)
        k = 1
        resultDict = {}
        frequentSet = getFrequentSinglet(data, s)
        candidateSet = frequentSet

        while(frequentSet!= set([])):
            #print("candidate set: ", candidateSet)
            #print("frequent set: ", frequentSet)
            resultDict[k] = frequentSet
            k += 1
            candidateSet = joinSet(frequentSet, k)
            frequentSet = getFrequentSet(candidateSet, data, s)
    
        res = []
        for val in resultDict.values():
            for i in list(val):
                 res.append(i)
        #print("stage 1 res : ", res)
        return res


    def joinSet(itemset, k):
        return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k]) 


    def getFrequentSinglet(data, s):
        count = collections.defaultdict(int)
        for line in data:
            for key in line:
                count[key] += 1
       
        res = set()
        s = support * len(data)
        for key, value in count.items():
            if (value >= s):
                tempset = frozenset([key])
                res.add(tempset)    
        return res

    def getFrequentSet(candidateSet, data, s):
        local = collections.defaultdict(int)
        resSet = set()
        for candidate in candidateSet:      
            for line in data:
                #pairs = set(itertools.combinations(line, k))
                lineSet = frozenset(line)
                if (candidate.issubset(lineSet)):
                    local[candidate] += 1

        for key, val in local.items():
            if (val >= s):
                resSet.add(key)     
        return resSet


    def stage2(data,candidateList):
        s = support * len(data)
        res = []
        '''
        resSet = set()
        for k in range(1, len(data)):
            tempSet = getFrequentSet(candidateSet, data, s, k)
            resSet.update(tempSet)
        resSet = set()
        for candidateSet in candidateList:
            print("candidateSet:", candidateSet)
            tempSet = getFrequentSet(candidateSet, data, s)
            print("tempSet:", tempSet)
            resSet.update(tempSet)
        '''
        resSet = getFrequentSet(candidateList, data, s)
        #print("res set: ", resSet)
        res = list()
        for l in resSet:
            l =  sorted(list(l))
            res.append(l)
        #res = sorted(res, key = lambda x: (len(x), x))
        #print(res)
        return res 


    lines = sc.textFile(sys.argv[1]).map(lambda line: line.encode("ascii", "ignore").split(",")).map(lambda x: list(int(y) for y in x))
    stage1Res = lines.mapPartitions(stage1).distinct()
    #print(stage1Res.collect())
    #stage1Res = stage1(lines.collect())
    #print("stage1Res: " , stage1Res)
    data = lines.collect()
    #stage2Res = stage2(data, stage1Res)
    #print(stage2Res)
    stage2Res = stage1Res.mapPartitions(lambda x: stage2(data, x))
    stage2Res = stage2Res.collect()
    stage2Res = sorted(stage2Res, key = lambda x: (len(x), x))

    text_file = open(sys.argv[3], "w")
    for line in stage2Res:
        if (len(line) == 1):
            text_file.write(str(line[0]) + "\n")
        else :
            res = "("
            for item in line:
                res += str(item) + ","
            res = res[:-1]
            res += ")"
            text_file.write(res + "\n")

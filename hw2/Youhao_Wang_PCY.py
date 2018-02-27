import sys
import collections
import os

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usrge: Youhao_Wang_PCY.py <Input-file>  <a> <b> <N> <s>  <Output-dir>", file=sys.stderr)
        exit(-1)
    a = int(sys.argv[2])
    b = int(sys.argv[3])
    n = int(sys.argv[4])
    s = int(sys.argv[5])

    # Pass One
    inputfile = open(sys.argv[1])
    baskets = []
    singletons = []
    frequent = collections.defaultdict(int)
    lines = inputfile.readlines()
    for line in lines:
        line=line.strip().split(",")
        baskets.append(line)
        for item in line:
            item = int(item)
            frequent[item] += 1
    frequent = collections.OrderedDict(sorted(frequent.items()))
    
    #Genrate the singletons 
    for key, val in frequent.items():
      if(val >= s):
         singletons.append(key)

    #Genrate the Candidate pairs count
    pairsCount = {}
    pairs = []
    for i in range(0, len(singletons)):
        for j in range(i + 1, len(singletons)):
             pair = [singletons[i], singletons[j]]
             pairs.append(pair)
             key = str(singletons[i]) + "," + str(singletons[j])
             pairsCount[key] = 0
    
    #Hashed pairs
    buckets = [0] * n
    for line in baskets:
        for i in range(0, len(line)):
            for j in range(i + 1, len(line)):
                hashValue = (a*int(line[i]) + b*int(line[j])) % n
                buckets[hashValue] += 1
                key = line[i] + "," + line[j]
                if key in pairsCount.keys():
                    pairsCount[key] += 1    

    #geneate the bitmap
    bitMap = 0
    fpCount = 0
    for i in range(0, len(buckets)):
        if (buckets[i] >= s):
            bitMap += 1 << i
            fpCount += 1




    #Pass Two
    candidatesPairs = []
    resultPairs = []
    fpRate = (float)(fpCount / len(buckets)) 


    #Generate the candidatesPair from Hashing
    for pair in pairs:
        hashValue = (a * pair[0] + b * pair[1]) % n
        if (bitMap & ( 1 << hashValue) != 0): 
            candidatesPairs.append(pair)
    sorted(candidatesPairs, key = lambda x: (x[0], x[1]))

    #Generate the result
    for pair in candidatesPairs:
        key = str(pair[0]) + "," + str(pair[1])
        if (pairsCount[key] >= s):
            resultPairs.append(pair)

    #output
    print ("False Positive Rate: ", "%.3f" % fpRate)
    canOut = sys.argv[6] + "/candidates.txt"
    os.makedirs(os.path.dirname(canOut), exist_ok=True)
    can_file = open(canOut, "w")
    for line in candidatesPairs:
        can_file.write("(" + str(line[0]) + "," + str(line[1]) + ")" + "\n")
            
    freOut = sys.argv[6] + "/frequentset.txt"
    os.makedirs(os.path.dirname(freOut), exist_ok=True)
    fre_file = open(freOut, "w")
    for line in singletons:
        fre_file.write(str(line) + "\n")
    for line in resultPairs:
        fre_file.write("(" + str(line[0]) + "," + str(line[1]) + ")" + "\n")

  


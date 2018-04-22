import networkx as nx
import numpy as np
import sys
#import matplotlib.pyplot as plt

if __name__ == "__main__":
    inputfile = open(sys.argv[1], 'rb')
    k = int(sys.argv[2])
    G = nx.Graph()

    G = nx.read_adjlist(inputfile, nodetype=int)
    #print(list(G.edges))
    L = nx.laplacian_matrix(G)
    #print(L)
    res = []    
    nodes = sorted(list(G.nodes))
    print(nodes)

    for i in range(k):
        vals, vects = np.linalg.eig(L.todense())
        #print(vals, vects)
        second_vector = vects[:,1]
        print(second_vector)
        print(sum(second_vector))

        negative = []
        positive = []
        for j in range(len(second_vector)):
            #if(j in res):
            #    continue
            if(second_vector[j] > 0):
                positive.append(nodes[j])
            else:
                negative.append(nodes[j])
    
        if(len(negative) > len(positive)):
            res.append(positive)
            sorted(res, key = len)
            if(len(res[0]) > len(positive):

            G.remove_nodes_from(positive)
        else:
            res.append(negative)
            G.remove_nodes_from(negative) 
        L = nx.laplacian_matrix(G)
        
    print(res)



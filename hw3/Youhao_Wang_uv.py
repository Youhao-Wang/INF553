import sys
import collections
import os
import numpy as np
import math

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usrge: Youhao_Wang_uv.py ratings-file n m f k")
        exit(-1)
    n = int(sys.argv[2])
    m = int(sys.argv[3])
    f = int(sys.argv[4])
    k = int(sys.argv[5])

    M = np.zeros((n, m))
    M[:] = np.nan
    U = np.ones((n, f))
    V = np.ones((f, m))


    inputfile = open(sys.argv[1])
    lines = inputfile.readlines()
    for line in lines:
        line=line.strip().split(",")
        if (line[0] == "userId"):
            continue
        userid = int(line[0]) - 1
        itemid = int(line[1]) - 1
        rate = float(line[2])
        if(userid < n and itemid < m):
            M[userid][itemid] = rate

    nonblack = np.count_nonzero(~np.isnan(M))
    for iteration in range(k):
        for r in range(n):
            for s in range(f):
                temp1 = M[r] - np.dot(U[r], V) + U[r][s] * V[s]
                temp2 = temp1 * V[s]
                U[r][s] = np.nansum(temp2) / np.nansum(np.square(V[s]))
                #print (U[r][s])

        for r in range(f):
            for s in range(m):
                temp1 = M[:, s] - np.dot(U, V[:, s]) + V[r][s] * U[:, r]
                #print(temp1.shape, U[:, r].shape)
                temp2 = temp1 * U[:, r]
                V[r][s] = np.nansum(temp2) / np.nansum(np.square(U[:,r]))

        M1 = np.dot(U, V)
        dis = np.square(abs(M1 - M))
        dis_sum = np.nansum(dis)
        RMSE = math.sqrt(dis_sum / nonblack)
        print ("%.4f" %RMSE)

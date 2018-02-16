#!/usr/bin/python3

from networkit import *
import glob

for n in [1000000, 2000000, 4000000, 8000000,16000000]:
    print("Graph with n=", n)
    print("Reading graph...")
    G = graphio.ThrillGraphBinaryReader().read(sorted(glob.glob("/amd.home/algoDaten/zeitz/graphs/mu-04/graph_50_10000_mu_0.4_{}-sorted-preprocessed-*.bin".format(n))))
    print("Reading partition...")
    P = community.BinaryEdgeListPartitionReader().read(sorted(glob.glob("/amd.home/algoDaten/zeitz/graphs/mu-04/part_50_10000_mu_0.4_{}-sorted-preprocessed-000*".format(n))))

    print("Coarsening...")
    coarsening = community.ParallelPartitionCoarsening(G, P, True).run()
    cG = coarsening.getCoarseGraph()
    print("Clustering...")
    cLouvainP = community.PLM(cG, par="none randomized").run().getPartition()
    print("Modularity of the detected clustering: ", community.Modularity().getQuality(cLouvainP, cG))
    print("Modularity of the ground truth: ", community.Modularity().getQuality(P, G))
    louvainP = community.PLM.prolong(cG, cLouvainP, G, coarsening.getFineToCoarseNodeMapping())
    print("ARI with ground truth: ", 1.0 - community.AdjustedRandMeasure().getDissimilarity(G, P, louvainP))

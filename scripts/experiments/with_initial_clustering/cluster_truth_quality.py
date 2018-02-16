#!/usr/bin/python3

from networkit import *
import glob

for imu in range(1,10):
    mu = imu/10

    print("mu=", mu)
    print("Reading graph...")
    G = graphio.ThrillGraphBinaryReader().read("/amd.home/algoDaten/zeitz/graphs/LFR_quality/graph_g2_b1_m0.{}.bin_graph".format(imu))
    print("Reading partition...")
    P = community.BinaryEdgeListPartitionReader().read("/amd.home/algoDaten/zeitz/graphs/LFR_quality/graph_g2_b1_m0.{}.bin_part".format(imu))

    print("Coarsening...")
    coarsening = community.ParallelPartitionCoarsening(G, P, True).run()
    cG = coarsening.getCoarseGraph()
    print("Clustering...")
    cLouvainP = community.PLM(cG, par="none randomized").run().getPartition()
    print("Modularity of the detected clustering: ", community.Modularity().getQuality(cLouvainP, cG))
    print("Modularity of the ground truth: ", community.Modularity().getQuality(P, G))
    louvainP = community.PLM.prolong(cG, cLouvainP, G, coarsening.getFineToCoarseNodeMapping())
    print("ARI with ground truth: ", 1.0 - community.AdjustedRandMeasure().getDissimilarity(G, P, louvainP))

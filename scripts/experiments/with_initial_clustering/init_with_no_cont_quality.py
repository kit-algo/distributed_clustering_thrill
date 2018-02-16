#!/usr/bin/python3

from networkit import *
import glob

for imu in range(1,10):
    mu = imu/10

    print("mu=", mu)
    print("Reading graph...")
    G = graphio.ThrillGraphBinaryReader().read(sorted(glob.glob("data/graphs/lfr_params/graph_g2_b1_m0.{}-preprocessed-*.bin".format(imu))))
    print("Reading ground truth partition...")
    P_gt = community.BinaryEdgeListPartitionReader().read(sorted(glob.glob("data/graphs/lfr_params/part_graph_g2_b1_m0.{}-preprocessed-*.bin".format(imu))))
    print("Reading DSLM-Mod without Contraction partition...")
    P = community.BinaryEdgeListPartitionReader().read(sorted(glob.glob("data/results/lfr_params/dlslm_no_contraction/clusterings/graph_g2_b1_m0.{}.run5-*.bin".format(imu))))

    print("Coarsening...")
    coarsening = community.ParallelPartitionCoarsening(G, P, True).run()
    cG = coarsening.getCoarseGraph()
    print("Clustering...")
    cLouvainP = community.PLM(cG, par="none randomized").run().getPartition()
    print("Modularity of the initial clustering: ", community.Modularity().getQuality(P, G))
    print("Modularity of the detected clustering: ", community.Modularity().getQuality(cLouvainP, cG))
    print("Modularity of the ground truth: ", community.Modularity().getQuality(P_gt, G))
    louvainP = community.PLM.prolong(cG, cLouvainP, G, coarsening.getFineToCoarseNodeMapping())
    print("ARI with ground truth: ", 1.0 - community.AdjustedRandMeasure().getDissimilarity(G, P_gt, louvainP))

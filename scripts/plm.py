#!/usr/bin/env python3

import sys

from networkit import graphio, community
from glob import glob

g = graphio.ThrillGraphBinaryReader().read(sorted(glob(sys.argv[1])))
c = community.detectCommunities(g)

community.writeCommunities(c, sys.argv[2])

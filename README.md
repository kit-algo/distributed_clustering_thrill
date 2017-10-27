# Distributed Graph Clustering using Thrill

This repository contains the code of the master thesis of [Tim Zeitz](https://github.com/SDEagle) [Engineering Distributed Graph Clustering using MapReduce](https://i11www.iti.kit.edu/_media/teaching/theses/ma-zeitz-17.pdf) and the follow up paper [Simple Distributed Graph Clustering using Modularity and Map Equation](https://arxiv.org/abs/1710.09605) submitted to IPDPS 2018.
The algorithms were implemented using the experimental [Thrill framework](https://github.com/thrill/thrill), though we maintained our own [fork](https://github.com/SDEagle/thrill) of it.
Most of the updates from the fork have now been merged back into upstream, but some discrepencies remain.
The code uses `C++14`, so you will need a recent `gcc` or `clang` to build.

## Building & Running

* clone the git repository
* run `git submodule update --init --recursive` to fetch dependencies
* create a folder for build output `mkdir build` and `cd` into it
* run `cmake ..` - this will create a makefile for a debug build - append `-DCMAKE_BUILD_TYPE=Release` for a optimized build without asserts
* run `make` to actually build everything
* run one of the binaries, eg `./dlslm_map_eq somegraph.gr`

### Pitfalls

Currently the `CMakeLists.txt` file blindly passes `-mavx2 -mfma` without checking if the system supports them.
If you get `Illegal Instruction` errors when running the binaries remove these flags (or switch to `-march=native`), they are not required and just make stuff faster.

## Content

The repo contains the following folders:

* `lib` - dependencies as git submodules
* `src` - actual source code
* `scripts` - scripted stuff to run our experiements, put moab jobs into a queue, etc.
* `analysis` - jupyter notebooks to analyse and plot the results of the experiments

When building everything (among others) the following binaries are build:

* `dlslm` - Distributed Synchronous Local Moving with Modularity
* `dlslm_map_equation` - Distributed Synchronous Local Moving with Map equation
* `preprocess` - Translate an arbitrary input graph into our custom binary format and perform some fixes along the way
* `streaming_clustering_analyser` - a utility to calculate map equation and modularity scores of clusterings on very large graphs
* `seq_louvain` - Graph Clustering using the original Louvain algorithm
* `infomap` - a wrapper binary around infomap to make the interface compatible with the on of `seq_louvain`

Our programs can read DIMACs graphs, SNAP Edge List graphs and our own custom binary format.
For optimal performance preprocess all graphs using the `preprocess` tool.
`seq_louvain` and `infomap` will output a help about command line arguments, for the other binaries you will have to refer to the source code or the scripts to see what arguments can be passed.

The analysis scripts/notebooks make use of [Networkit](https://github.com/kit-parco/networkit) which you can also use to generate some test graphs.
To be able to read our binary graphs you will need to use the thrill_support branch of this [fork](https://github.com/michitux/networkit/tree/thrill_support) of networkit.

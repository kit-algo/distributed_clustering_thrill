#!/bin/bash
#MSUB -N gossip map graph conversion
#MSUB -l pmem=4000mb
#MSUB -q singlenode

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} tasks"

executable="$HOME/code/release/convert_graph_to_gossipmap_binary_edgelist"
echo $executable "${GRAPH}" ${OUT}
exec $executable "${GRAPH}" ${OUT}

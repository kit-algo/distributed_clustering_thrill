#!/bin/bash
#MSUB -N seq_louvain
#MSUB -l nodes=2:ppn=28
#MSUB -l pmem=4570mb
#MSUB -q multinode

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores"

executable="$HOME/code/release/infomap"
echo $executable -b "$GRAPH" -o "$CLUSTERING-$MOAB_JOBID.part"
exec $executable -b "$GRAPH" -o "$CLUSTERING-$MOAB_JOBID.part"

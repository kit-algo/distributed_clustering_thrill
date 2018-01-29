#!/bin/bash
#MSUB -N relax_map
#MSUB -l nodes=2:ppn=28
#MSUB -l pmem=4570mb
#MSUB -q multinode

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores"

executable="$HOME/RelaxMap/ompRelaxmap"
seed=$(ruby -e "puts rand 2**31 - 1")
echo $executable "$seed" "$GRAPH" 16 1 1e-3 0.0 10 "$CLUSTERING-$MOAB_JOBID.part" prior
exec $executable "$seed" "$GRAPH" 16 1 1e-3 0.0 10 "$CLUSTERING-$MOAB_JOBID.part" prior

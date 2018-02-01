#!/bin/bash
#MSUB -N plm
#MSUB -l nodes=2:ppn=28
#MSUB -l pmem=4570mb
#MSUB -q multinode

module load compiler/gnu/7.1
module load devel/python/3.5.2

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores"

export PYTHONPATH="$PYTHONPATH:$HOME/networkit"
executable="$HOME/code/scripts/plm.py"
echo $executable "$GRAPH" "$CLUSTERING-$MOAB_JOBID.part.txt"
exec $executable "$GRAPH" "$CLUSTERING-$MOAB_JOBID.part.txt"

#!/bin/bash
#MSUB -N seq_louvain
#MSUB -l nodes=2:ppn=28
#MSUB -l pmem=4500mb
#MSUB -q multinode

module load mpi/openmpi/2.0-gnu-5.2
cat /proc/cpuinfo > cpu-$MOAB_JOBID

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores"

executable="$HOME/code/prototypes/louvain/release/seq_louvain"
echo $executable "-b $GRAPH" "$CLUSTERING-$MOAB_JOBID.part"
exec $executable "-b $GRAPH" "$CLUSTERING-$MOAB_JOBID.part"

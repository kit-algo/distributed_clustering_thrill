#!/bin/bash
#MSUB -N preprocess
#MSUB -l pmem=4570mb
#MSUB -q multinode
#MSUB -v THRILL_WORKERS_PER_HOST=16
#MSUB -v THRILL_RAM=120GiB

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

executable="$HOME/code/release/preprocess"
startexe="$HOME/code/lib/thrill/run/slurm/invoke.sh ${executable}"
echo $startexe "$GRAPH" "$GROUNDTRUTH"
exec $startexe "$GRAPH" "$GROUNDTRUTH"
rm "$GRAPH" "$GROUNDTRUTH"

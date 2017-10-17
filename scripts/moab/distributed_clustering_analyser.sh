#!/bin/bash
#MSUB -N distributed clustering analyser
#MSUB -l pmem=4570mb
#MSUB -q multinode
#MSUB -v THRILL_WORKERS_PER_HOST=8
#MSUB -v THRILL_RAM=120GiB
#MSUB -v THRILL_BLOCK_SIZE=131072
#MSUB -v GLIBCPP_FORCE_NEW=1

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/release/distributed_clustering_analyser"
startexe="$HOME/code/lib/thrill/run/slurm/invoke.sh ${executable}"
echo $startexe "$GRAPH" "$CLUSTERING,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING,$result_id"

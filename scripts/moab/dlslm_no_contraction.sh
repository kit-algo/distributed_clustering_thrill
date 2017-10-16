#!/bin/bash
#MSUB -N synchronous modularity without contraction
#MSUB -l pmem=4570mb
#MSUB -q multinode
#MSUB -v THRILL_WORKERS_PER_HOST=16
#MSUB -v THRILL_RAM=120GiB
#MSUB -v GLIBCPP_FORCE_NEW=1

module load compiler/gnu/7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/release/dlslm_no_contraction"
startexe="$HOME/code/lib/thrill/run/slurm/invoke.sh ${executable}"
echo $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"
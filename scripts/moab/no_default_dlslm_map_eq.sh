#!/bin/bash
#MSUB -N synchronous map equation
#MSUB -l pmem=4500mb
#MSUB -q multinode
#MSUB -v THRILL_RAM=120GiB
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2

export THRILL_LOG="dlslm_map_eq-log-${MOAB_JOBID}"
module load ${MPI_MODULE}
MPIRUN_OPTIONS="--bind-to core --map-by node:PE=$((MOAB_PROCCOUNT / MOAB_NODECOUNT)) -report-bindings"

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/release/dlslm_map_eq"
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${executable}"
echo $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"

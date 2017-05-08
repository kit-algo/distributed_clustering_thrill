#!/bin/bash
#MSUB -N distributed_local_moving
#MSUB -l walltime=01:00:00
#MSUB -l nodes=4:ppn=17
#MSUB -l mem=512000mb
#MSUB -q multinode
#MSUB -v THRILL_WORKERS_PER_HOST=17
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2

cat /proc/cpuinfo

export THRILL_LOG="$HOME/results/pre_dlm/dlm-log-${MOAB_JOBID}"
module load ${MPI_MODULE}
MPIRUN_OPTIONS="--bind-to core --map-by node:PE=$((MOAB_PROCCOUNT / MOAB_NODECOUNT)) -report-bindings"

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${EXECUTABLE}"
echo $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"

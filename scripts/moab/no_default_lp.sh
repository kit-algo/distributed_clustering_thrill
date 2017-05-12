#!/bin/bash
#MSUB -N label_prop
#MSUB -l pmem=4500mb
#MSUB -q multinode
#MSUB -v THRILL_RAM=120GiB
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2

cat /proc/cpuinfo > cpu-$MOAB_JOBID

export THRILL_LOG="lp-log-${MOAB_JOBID}"
module load ${MPI_MODULE}
MPIRUN_OPTIONS="--bind-to core --map-by node:PE=$((MOAB_PROCCOUNT / MOAB_NODECOUNT)) -report-bindings"

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

executable="$HOME/code/prototypes/thrill_louvain/release/label_prop"
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${executable}"
echo $startexe "$GRAPH"
exec $startexe "$GRAPH"

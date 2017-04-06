#!/bin/bash
#MSUB -N distributed_local_moving
#MSUB -l nodes=4:ppn=5
#MSUB -l walltime=01:00:00
#MSUB -l pmem=12000mb
#MSUB -q multinode
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2
#MSUB -v THRILL_WORKERS_PER_HOST=5
#MSUB -v MPIRUN_OPTIONS="--bind-to core --map-by socket:PE=5 -report-bindings"

export THRILL_LOG="dlm-log-${MOAB_JOBID}"
module load ${MPI_MODULE}

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/prototypes/thrill_louvain/build/node_based_local_moving"
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${executable}"
echo $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID.bin,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID.bin,$result_id"

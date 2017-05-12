#!/bin/bash
#MSUB -N analyze_ground_truth
#MSUB -l walltime=01:00:00
#MSUB -l nodes=16:ppn=28
#MSUB -l pmem=4500mb
#MSUB -q multinode
#MSUB -v THRILL_WORKERS_PER_HOST=17
#MSUB -v THRILL_RAM=120GiB
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2

module load ${MPI_MODULE}
MPIRUN_OPTIONS="--bind-to core --map-by node:PE=$((MOAB_PROCCOUNT / MOAB_NODECOUNT)) -report-bindings"

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

clustering_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/prototypes/thrill_louvain/release/distributed_clustering_analyser"
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${executable}"
echo $startexe "$GRAPH" "$GROUNDTRUTH,$clustering_id"
exec $startexe "$GRAPH" "$GROUNDTRUTH,$clustering_id"

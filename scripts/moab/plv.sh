#!/bin/bash
#MSUB -N partitioned_louvain
#MSUB -l nodes=4:ppn=17
#MSUB -l walltime=01:00:00
#MSUB -l mem=512000mb
#MSUB -q multinode
#MSUB -v MPI_MODULE=mpi/openmpi/2.0-gnu-5.2
#MSUB -v THRILL_WORKERS_PER_HOST=17
#MSUB -v MPIRUN_OPTIONS="--bind-to core --map-by node:PE=17 -report-bindings"

cat /proc/cpuinfo

export THRILL_LOG="plv-log-${MOAB_JOBID}"
module load ${MPI_MODULE}

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} MPI-tasks and ${THRILL_WORKERS_PER_HOST} threads"

result_id=$(ruby -e "require 'securerandom'; puts SecureRandom.uuid")
executable="$HOME/code/prototypes/thrill_louvain/release/partitioned_louvain"
startexe="mpirun -n ${MOAB_NODECOUNT} ${MPIRUN_OPTIONS} ${executable}"
echo $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"
exec $startexe "$GRAPH" "$CLUSTERING-$MOAB_JOBID-@@@@-#####.bin,$result_id"

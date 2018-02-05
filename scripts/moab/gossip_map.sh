#!/bin/bash
#MSUB -N gossip map
#MSUB -l nodes=2:ppn=28
#MSUB -l walltime=00:20:00
#MSUB -l pmem=4570mb
#MSUB -q multinode

module load mpi/openmpi/2.1-gnu-7.1

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} tasks and 16 threads"

free -h
~/spectre-meltdown-checker.sh

mpi_options="--bind-to core --map-by node:PE=16 -report-bindings"
executable="$HOME/gossip_map/release/apps/GossipMap/GossipMap"
startexe="mpirun -n ${MOAB_NODECOUNT} ${mpi_options} ${executable}"
echo $startexe --format bintsv4 --graph ${GRAPH} --prefix "${OUT_PREFIX}-${MOAB_JOBID}" --ncpus 16
exec $startexe --format bintsv4 --graph ${GRAPH} --prefix "${OUT_PREFIX}-${MOAB_JOBID}" --ncpus 16

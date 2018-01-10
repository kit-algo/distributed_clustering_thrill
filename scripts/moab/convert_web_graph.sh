#!/bin/bash
#MSUB -N convert web graph
#MSUB -l nodes=1:ppn=1
#MSUB -l mem=64000mb
#MSUB -l walltime=05:00:00
#MSUB -q singlenode

echo "${MOAB_JOBNAME} running on ${MOAB_PROCCOUNT} cores with ${MOAB_NODECOUNT} tasks and ${THRILL_WORKERS_PER_HOST} threads"

executable="java -jar $HOME/webgraphconverter-1.0-SNAPSHOT.jar"
echo $executable "$WORK/eu-2015-hc" "$WORK/eu-2015.graph"
exec $executable "$WORK/eu-2015-hc" "$WORK/eu-2015.graph"

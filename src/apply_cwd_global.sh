#!/bin/bash
# this is for sumitting the job on a single machine (node) with multiple cores
Rscript analysis/apply_cwd_global.R 1 1

# # this is for submitting jobs on HPC with a queueing system
# TODO: Note by Fabian: only get_cwd_annmax.R is setup for multi-node parallelization but not apply_cwd_global.R
# TODO: Note by Fabian: only  Furthermore, no HPC available with bsub.
# njobs=4   # number of top-level jobs, sent to separate nodes. On each node, number fo cores is automatically determined and used.
# for ((n=1;n<=${njobs};n++)); do
#     echo "Submitting chunk number $n ..."
#     bsub -W 72:00 -u bestocke -J "job_name $n" -R "rusage[mem=10000]" "Rscript vanilla analysis/apply_cwd_global.R $n $njobs"
# done

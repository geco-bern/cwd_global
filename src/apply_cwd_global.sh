#!/bin/bash
nlon=288  # set this by hand: is the number of longitude indices in your files. All files must have the same spatial grid.
njobs=4   # number of top-level jobs, sent to separate nodes. On each node, number fo cores is automatically determined and used.

# this is for sumitting the job on a single machine (node) with multiple cores
Rscript analysis/apply_cwd_global.R 1 1 $nlon

# # this is for submitting jobs on HPC with a queueing system
# for ((n=1;n<=${njobs};n++)); do
#     echo "Submitting chunk number $n ..."
#     bsub -W 72:00 -u bestocke -J "job_name $n" -R "rusage[mem=10000]" "Rscript vanilla analysis/apply_cwd_global.R $n $njobs $nlon"
# done

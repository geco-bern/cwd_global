#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_pcwd_40Gx4coresx200jobs"
#SBATCH --time=6-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1             # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --array=1-200%12       # specifies the slurm array job with the number of tasks (at most 12 running simultaneously to restrict hogging of our nodes (12*160GB = 1900GB with 2x1500GB leaving 1100GB free))
#SBATCH --cpus-per-task=4      # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=30G           # First jobs showed it takes about 77GB, we thus request NCPU x 30GB = 120GB
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=fail,begin,end            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land-fullResNoNA  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

module load CMake/3.31.3-GCCcore-14.2.0
module load UDUNITS/2.2.28-GCCcore-14.2.0
module load PROJ/9.6.2-GCCcore-14.2.0 # needed for 02_apply_pcwd...
module load GDAL/3.11.1-foss-2025a    # needed for 02_apply_pcwd...
module load GEOS/3.13.1-GCC-14.2.0
module load netCDF/4.9.3-gompi-2025a
module load R/4.5.1-gfbf-2025a

## Run the R scripts
# Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 1  # call with command line arguments 1 1, using only multi-core but no multi-node parallelization
Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R $SLURM_ARRAY_TASK_ID $SLURM_ARRAY_TASK_MAX # call with multi-node parallelization

echo "Finished on: $(date --rfc-3339=seconds)"


# 02_apply_pcwd_global_ERA5Land_ubelix.R requires these resources:

# (base) fb24k097@submit04:~$ seff 46223655_01
# Job ID: 46223656
# Array Job ID: 46223655_1
# Cluster: ubelix
# User/Group: fb24k097/giub
# State: COMPLETED (exit code 0)
# Nodes: 1
# Cores per node: 4
# CPU Utilized: 13:59:15
# CPU Efficiency: 87.26% of 16:01:48 core-walltime
# Job Wall-clock time: 04:00:27
# Memory Utilized: 124.57 GB
# Memory Efficiency: 77.86% of 160.00 GB (40.00 GB/core)

# (base) fb24k097@submit04:~$ seff 46223655_40
# Job ID: 46269964
# Array Job ID: 46223655_40
# Cluster: ubelix
# User/Group: fb24k097/giub
# State: COMPLETED (exit code 0)
# Nodes: 1
# Cores per node: 4
# CPU Utilized: 20:53:34
# CPU Efficiency: 88.62% of 23:34:32 core-walltime
# Job Wall-clock time: 05:53:38
# Memory Utilized: 126.06 GB
# Memory Efficiency: 78.79% of 160.00 GB (40.00 GB/core)

# (base) fb24k097@submit04:~/GitHub/geco-bern/cwd_global(full-resolution-ERA5-2018-2020)$ seff 46328017_130
# Job ID: 46423725
# Array Job ID: 46328017_130
# Cluster: ubelix
# User/Group: fb24k097/giub
# State: COMPLETED (exit code 0)
# Nodes: 1
# Cores per node: 4
# CPU Utilized: 09:29:28
# CPU Efficiency: 89.60% of 10:35:36 core-walltime
# Job Wall-clock time: 02:38:54
# Memory Utilized: 131.11 GB
# Memory Efficiency: 81.95% of 160.00 GB (40.00 GB/core)
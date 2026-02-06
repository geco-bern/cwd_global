#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_pcwd_40Gx4coresx90nodes_compressed"
#SBATCH --time=6-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --array=1-90           # specifies the slurm array job with the number of tasks
#SBATCH --cpus-per-task=4      # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=40G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=fail            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land-fullRes  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

module load UDUNITS/2.2.28-GCCcore-13.3.0
module load PROJ/9.4.1-GCCcore-13.3.0 # needed for 02_apply_pcwd...
module load GDAL/3.10.0-foss-2024a    # needed for 02_apply_pcwd...
module load netCDF
module load R

## Run the R scripts
## If you don't provide a chdir argument to SLURM, provide to full path from your HOME folder.
## Rscript GitHub/fabern/parallelization-tests/01_example_future.R
## If you provide a chdir argument to SLURM, provide the relative path
# Rscript 01_make_tidy_ERA5Land.R   # call without any command line arguments

## This was then followed by manual copying of map2tidy results from temporary scratch to capacity storage:
## ssh ubelix; tmux; rsync --dry-run --human-readable -i --info=progress2 -av --no-perms --no-owner --no-group /storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy1950-2024 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy1950-2024

# Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 1  # call with command line arguments 1 1, using only multi-core but no multi-node parallelization
Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R $SLURM_ARRAY_TASK_ID $SLURM_ARRAY_TASK_COUNT # call with multi-node parallelization

## This was then followed by manual copying of pcwd results from temporary scratch to capacity storage:
## ssh ubelix; tmux; rsync --dry-run --human-readable -i --info=progress2 -av --no-perms --no-owner --no-group TODO TODO TODO

echo "Finished on: $(date --rfc-3339=seconds)"

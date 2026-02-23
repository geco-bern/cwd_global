#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_pcwd_create_parquet"
#SBATCH --time=1-12:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=34       # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=13G
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
Rscript 03_collect_pcwd_results_to_parquet.R

echo "Starting rsync on: $(date --rfc-3339=seconds)"
src_file="/scratch/local/${SLURM_JOB_ID}/data_derived_03_daily_pcwd.parquet/"
dst_file="/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_rsynced.parquet"
rsync -avz "$src_file" "$dst_file"

echo "Finished on: $(date --rfc-3339=seconds)"

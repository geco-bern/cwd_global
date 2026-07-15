#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_tidy_2Gx190coresx1jobs"
#SBATCH --time=3-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1             # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=80     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=2G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=fail       # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
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
Rscript 01_make_tidy_ERA5Land.R   # call without any command line arguments
echo "Finished on: $(date --rfc-3339=seconds)"

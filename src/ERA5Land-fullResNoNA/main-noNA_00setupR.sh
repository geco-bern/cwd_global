#! /usr/bin/bash -l
#SBATCH --job-name="main-noNA_main-noNA_00setupR.sh"
#SBATCH --time=4-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=8     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=4G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=begin,end,fail            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land-fullResNoNA  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary

# INSTRUCTIONS: ssh ubelix; cd ~/GitHub/geco-bern/cwd_global; sbatch src/ERA5Land-fullResNoNA/main-noNA_00setupR.sh

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

module purge
module load GCCcore/13.3.0
module load cURL/8.7.1-GCCcore-13.3.0
module load libxml2/2.12.7-GCCcore-13.3.0
module load libarchive/3.7.4-GCCcore-13.3.0
module load ICU/75.1-GCCcore-13.3.0
module load CMake/3.29.3-GCCcore-13.3.0
module load UDUNITS/2.2.28-GCCcore-13.3.0
module load PROJ/9.4.1-GCCcore-13.3.0 # needed for 02_apply_pcwd...
module load GDAL/3.10.0-foss-2024a    # needed for 02_apply_pcwd...
module load GEOS/3.12.2-GCC-13.3.0
module load netCDF/4.9.2-gompi-2024a
module load R/4.4.2-gfbf-2024a

echo "Initialize renv(): $(date '+%Y-%m-%d_%H-%M-%S')"
Rscript -e 'source("renv/activate.R"); renv::status()'
Rscript -e 'source("renv/activate.R"); renv::restore(prompt = FALSE)'
#Rscript -e 'source("renv/activate.R"); renv::restore(rebuild = TRUE, prompt = FALSE)'

echo "Finished on: $(date --rfc-3339=seconds)"

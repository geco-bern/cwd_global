#! /usr/bin/bash -l
#SBATCH --job-name="00_setup_R"
#SBATCH --time=4-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=8     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=4G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=begin,end,fail            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land-lowRes  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary
#SBATCH --output=slurm-%x_%j.out        # --output relative to working directory (takes into account --chdir)

# INSTRUCTIONS: ssh ubelix; cd ~/GitHub/geco-bern/cwd_global; sbatch src/ERA5Land-lowRes/00_setup_R.sh

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

export RENV_CONFIG_PAK_ENABLED=false # important to prevent pak from doing: sudo /bin/bash -c id # https://github.com/r-lib/pak/issues/812
export PKG_SYSREQS=false             # important to prevent pak from doing: sudo /bin/bash -c id # https://github.com/r-lib/pak/issues/812
export PKG_SYSREQS_SUDO=false        # important to prevent pak from doing: sudo /bin/bash -c id # https://github.com/r-lib/pak/issues/812

module purge
module load GCCcore/14.2.0
module load cURL/8.11.1-GCCcore-14.2.0
module load libxml2/2.13.4-GCCcore-14.2.0
module load libarchive/3.7.7-GCCcore-14.2.0
module load ICU/76.1-GCCcore-14.2.0
module load CMake/3.31.3-GCCcore-14.2.0
module load UDUNITS/2.2.28-GCCcore-14.2.0
module load PROJ/9.6.2-GCCcore-14.2.0 # needed for 02_apply_pcwd...
module load GDAL/3.11.1-foss-2025a    # needed for 02_apply_pcwd...
module load GEOS/3.13.1-GCC-14.2.0
module load netCDF/4.9.3-gompi-2025a
module load R/4.5.1-gfbf-2025a

rm -r renv   # NOTE: be sure that this overwrites these files
rm renv.lock # NOTE: be sure that this overwrites these files
rm .Rprofile # NOTE: be sure that this overwrites these files

Rscript 00_setup_R.R   # call without any command line arguments, if you overwrite them anyway in the R script with 'args <- c(1,1)'

echo "Finished on: $(date --rfc-3339=seconds)"

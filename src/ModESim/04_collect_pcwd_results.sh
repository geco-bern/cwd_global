#! /usr/bin/bash -l
#SBATCH --job-name="collect PCWD annmax 1420 set"
#SBATCH --time=01:00:00
#SBATCH --partition=icpu-stocker # if you have access, this gives you priority
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=1      # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mail-user=patricia.helpap@students.unibe.ch
#SBATCH --mail-type=fail               # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=cwd_global/analysis/ModESim  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.
module load R

## Run a small test R script using parallel workers
##Rscript GitHub/fabern/parallelization-tests/01_example_future.R # If you don't provide a chdir argument to SLURM, provide to full path from your HOME folder.
Rscript 04_collect_pcwd_results_ModESim.R   # call without any command line arguments, if you overwrite them anyway in the R script with 'args <- c(1,1)'

echo "Finished on: $(date --rfc-3339=seconds)"

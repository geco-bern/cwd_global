#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_pcwd_create_NetCDF"
#SBATCH --time=20:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1                # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --array=1950-2024         # specifies the slurm array job with the number of tasks
#SBATCH --cpus-per-task=60        # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=4G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=fail            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land-fullRes  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

# load other modules
module load UDUNITS/2.2.28-GCCcore-13.3.0
module load PROJ/9.4.1-GCCcore-13.3.0 # needed for 02_apply_pcwd...
module load GDAL/3.10.0-foss-2024a    # needed for 02_apply_pcwd...
module load netCDF
module load R

module load Anaconda3
eval "$(conda shell.bash hook)"

# python environment setup
# manually create pre-defined environment
# and store in conda-environment.yml
# conda env export | grep -v "^prefix: " > ../../analysis/ERA5Land-fullRes/conda-environment.yml

# create pre-defined environment on compute node
# ======= run this at least once on UBELIX (to create ~/.conda/envs/cwd_era5land) =======
# conda env create -f ../../analysis/ERA5Land-fullRes/conda-environment.yml -n cwd_era5land

# activate environment
conda activate cwd_era5land
status=$?
if [ ! $status == "0" ]; then
    echo "need to create env"
fi


echo $SLURM_ARRAY_TASK_ID


## Move parquet files from capacity storage to node-local scratch
# Move (ONLY 1 year of) data to local files building a small test set:
mkdir -p "/scratch/local/input_${SLURM_ARRAY_TASK_ID}.parquet/year=${SLURM_ARRAY_TASK_ID}"
cp -r \
    /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_rsynced.parquet/year=${SLURM_ARRAY_TASK_ID}/LON_str=LON_%2B* \
    /scratch/local/input_${SLURM_ARRAY_TASK_ID}.parquet/year=${SLURM_ARRAY_TASK_ID}
# NOTE for development: 2B00* could be used to reduce the number of Longitudes included

## Run the python script
conda activate /storage/homefs/fb24k097/GitHub/geco-bern/cwd_global/.conda
python 04_collect_parquet_to_NetCDF.py --year $SLURM_ARRAY_TASK_ID --parquet_path /scratch/local/input_${SLURM_ARRAY_TASK_ID}.parquet --out_nc /scratch/local/output_${SLURM_ARRAY_TASK_ID}.nc4

## Move results to capacity storage
rsync /scratch/local/output_${SLURM_ARRAY_TASK_ID}.nc4 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_04_daily_pcwd_${SLURM_ARRAY_TASK_ID}.nc4

rm -r "/scratch/local/input_${SLURM_ARRAY_TASK_ID}.parquet"

echo "Finished on: $(date --rfc-3339=seconds)"

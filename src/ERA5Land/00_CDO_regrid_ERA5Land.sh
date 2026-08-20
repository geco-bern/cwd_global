#! /usr/bin/bash -l
#SBATCH --job-name="00_CDO_regrid_ERA5Land"
#SBATCH --time=4-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=8     # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=24G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=begin,end,fail            # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks
#SBATCH --chdir=../cwd_global/analysis/ERA5Land  # define here the working directory which contains your R-script, and where the output will be written to; no tilde ~/ necessary
#SBATCH --output=slurm-%x_%j.out        # --output relative to working directory (takes into account --chdir)

# INSTRUCTIONS: ssh ubelix; cd ~/GitHub/geco-bern/cwd_global; sbatch src/ERA5Land/00_CDO_regrid_ERA5Land.sh
# DEBUG: srun --qos job_icpu-stocker --account=invest -n 1 --cpus-per-task=12 --mem-per-cpu=12G --time=4-00:00:00 --pty bash

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

module load netCDF/4.9.3-gompi-2025a
module load CDO/2.4.4-gompi-2024a
# module load CMake/3.31.3-GCCcore-14.2.0
# module load UDUNITS/2.2.28-GCCcore-14.2.0
# module load PROJ/9.6.2-GCCcore-14.2.0 # needed for 02_apply_pcwd...
# module load GDAL/3.11.1-foss-2025a    # needed for 02_apply_pcwd...
# module load GEOS/3.13.1-GCC-14.2.0
# module load R/4.5.1-gfbf-2025a

# do first manually aggregation of hourly data to daily:
# ssh ubelix; cd ~; sbatch ~/GitHub/fabern/download-ECMWF-data/aggregate_ERA5land_UTCdaily.sh
# # which then should generate: /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily

                # THEN PREPARE THE DATA IN THE INPUT DIRECTORY SPECIFIED IN THE SEPARATE .sh FILES:
                # #ssh ubelix
                # #tmux
                # cd /storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_2020-2026
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_sp.2024.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_t2m.2024.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_ssr.2024.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_str.2024.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_tp.2024.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_sp.2023.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_t2m.2023.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_ssr.2023.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_str.2023.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_tp.2023.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_sp.2022.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_t2m.2022.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_ssr.2022.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_str.2022.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_tp.2022.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_sp.2021.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_t2m.2021.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_ssr.2021.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_str.2021.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_tp.2021.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_sp.2020.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.mean_t2m.2020.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_ssr.2020.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_str.2020.nc .
                # rsync -v --info=progress2 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/ERA5Land_UTCDaily.tot_tp.2020.nc .


# loop
for script in ../../src/CDO/ERA5-Land/regrid_*.sh; do
    # parallel version
    # bash "$script" & # & starts it in the background, so that the next script can be started immediately

    # serial version
    printf "$(date --rfc-3339=seconds): Starting script: %s\n" "$script"
    bash "$script"
done
wait # pauses the main job until every background job has finished

# then do calc_netrad, which uses regridded ssr and str
printf "$(date --rfc-3339=seconds): Starting script: %s\n" "calc_netrad.sh"
bash ../../src/CDO/ERA5-Land/calc_netrad.sh

# then do rename_netrad
printf "$(date --rfc-3339=seconds): Starting script: %s\n" "rename_netrad.sh"
bash ../../src/CDO/ERA5-Land/rename_netrad.sh

echo "Finished on: $(date --rfc-3339=seconds)"

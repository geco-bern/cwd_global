# Analysis workflow for different climate / reanalysis inputs
*Author: Benjamin Stocker*

## Workflow (CMIP6)

1.  `analysis/make_tidy_cmip6.R`: Make original global files tidy. Save as nested data frames, where each row is one grid cell and time series is nested in the column `data`. Separate files are written for each longitudinal band. Writes files:
    `evspsbl/evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_LON_[+-XXX.XXX].rds`
    into folder defined as `outdir`.

2.  `analysis/apply_cwd_global.R`: Script for parallel applying the CWD algorithm (or anything else that operates on full time series) separately for on gridcell. Distributes by longitudinal band. Reading files written in previous step and writing files (in the example):
    `cwd/CWD_result_LON_[+-XXX.XXX].rds`
    into folder defined as `outdir`.

    `src/apply_cwd_global.sh`: Bash script that calls `apply_cwd_global.R` , to be used as an alternative and containing submission statement for HPC.

    Note: This step creates data at the original temporal resolution. Data is not collected at this stage to avoid memory limitation.

3.  `analysis/get_cwd_annmax.R`: Script for parallel applying function for determining annual maximum. Reading files written in previous step and writing files (in the example):

    `cwd_annmax/CWD_result_LON_[+-XXX.XXX]_ANNMAX.rds`

4.  `collect_cwd_annmax.R`: Script for collecting annual time series of each gridcell and writing the global annual data into a NetCDF file. Since annual maximum is much smaller data and can now be handled by reading all into memory. This uses the function `write_nc2()` from the package {rgeco}. Install it from [here](https://github.com/geco-bern/rgeco). Writes single file containing global data with annual resolution as NetCDF:

    `/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd_annmax_global.nc`

Writes file containing global data with annual resolution: 
**Note**: Adjust paths and file name prefixes for your own case in scripts (located in subdirectory `analysis/`)


## Workflow (ModE-Sim and ERA5-Land)

Similar to CMIP6 workflow, with updated paths and functions.
The full workflow including pre-processing of data for ModE-Sim and ERA5-Land data is shown here and in Figure 4 in thesis: 

![image](https://github.com/user-attachments/assets/062a8469-950a-44fd-a1c4-9345515a6ec1)

Note that the above workflow (from Patricia's thesis) works with regridded ERA5-Land data sets.
The workflow has been applied again on the full data set with original spatial resolution.
This was done with the scripts `analysis/ERA5Land-fullRes/01_make_tidy_ERA5Land.R` and `analysis/ERA5Land-fullRes/02_apply_pcwd_global_ERA5Land_ubelix.R`.
Easiest is to run it on UBELIX by submitting the bash script `analysis/ERA5Land-fullRes/main.sh`. This can be done with `ssh ubelix; cd ~/GitHub/geco-bern/cwd_global/; sbatch src/ERA5Land-fullRes/main.sh`.
Followed by clean-up of tidy results: `ssh ubelix`,`tmux`, `rsync --dry-run --human-readable -i --info=progress2 -av --no-perms --no-owner --no-group /storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy1950-2024 /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_tidy_dailyUTC_v3/tidy1950-2024`
and clean-up of pcwd results: `ssh ubelix`,`tmux`, `rsync --dry-run --human-readable -i --info=progress2 -avz --no-perms --no-owner --no-group /storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/02_daily_pcwd/ /storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd`
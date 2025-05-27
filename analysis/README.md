![image](https://github.com/user-attachments/assets/1b1525be-403a-4bfd-9e74-7a97f96bd247)# cwd_global: Apply CWD algorithm to global files

*Author: Benjamin Stocker*

## Workflow

1.  `01_make_tidy_cmip6.R`: Make original global files tidy. Save as nested data frames, where each row is one grid cell and time series is nested in the column `data`. Separate files are written for each longitudinal band. Writes files:

    `~/data/cmip6-ng/tidy/evspsbl_mon_CESM2_historical_r1i1p1f1_native_LON_<+-XXX.XXX>.rds`

2.  `02_apply_cwd_global.R`: Script for parallel applying the CWD algorithm (or anything else that operates on full time series) separately for on gridcell. Distributes by longitudinal band. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>.rds`

    `apply_cwd_global.sh`: Bash script that calls `apply_cwd_global.R` , to be used as an alternative and containing submission statement for HPC.

    Note: This step creates data at the original temporal resolution. Data is not collected at this stage to avoid memory limitation.

3.  `03_get_cwd_annmax.R`: Script for parallel applying function for determining annual maximum. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>_ANNMAX.rds`

4.  `04_collect_cwd_results.R`: Script for collecting annual time series of each gridcell - is much smaller data and can now be handled by reading all into memory. Writes file containing global data with annual resolution as RDS and NetCDF file. This uses the function `write_nc2()` from the package {rgeco}. Install it from [here](https://github.com/geco-bern/rgeco):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_cum_ANNMAX.rds` and `~/data/cmip6-ng/tidy/cwd/evspsbl_cum_ANNMAX.nc`

### ModE-Sim and ERA5-Land data

The full workflow including pre-processing of data for ModE-Sim and ERA5-Land data is shown here: 

![image](https://github.com/user-attachments/assets/062a8469-950a-44fd-a1c4-9345515a6ec1)



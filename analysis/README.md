# cwd_global: Apply CWD algorithm to global files

*Author: Benjamin Stocker*

## Workflow

1.  `make_tidy_cmip6.R`: Make original global files tidy. Save as nested data frames, where each row is one grid cell and time series is nested in the column `data`. Separate files are written for each longitudinal band. Writes files:

    `~/data/cmip6-ng/tidy/evspsbl_mon_CESM2_historical_r1i1p1f1_native_LON_<+-XXX.XXX>.rds`

2.  `apply_cwd_global.R`: Script for parallel applying the CWD algorithm (or anything else that operates on full time series) separately for on gridcell. Distributes by longitudinal band. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>.rds`

    `apply_cwd_global.sh`: Bash script that calls `apply_cwd_global.R` , to be used as an alternative and containing submission statement for HPC.

    Note: This step creates data at the original temporal resolution. Data is not collected at this stage to avoid memory limitation.

3.  `get_cwd_annmax.R`: Script for parallel applying function for determining annual maximum. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>_ANNMAX.rds`

4.  `collect_cwd_annmax.R`: Script for collecting annual time series of each gridcell - is much smaller data and can now be handled by reading all into memory. Writes file containing global data with annual resolution:

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_cum_ANNMAX.rds`

5.  `create_nc_annmax.R`: Script for writing the global annual data into a NetCDF file. This uses the function `write_nc2()` from the package {rgeco}. Install it from [here](https://github.com/geco-bern/rgeco). Writes file containing global data with annual resolution as NetCDF:

    `~/data/cmip6-ng/tidy/cwd/evspsbl_cum_ANNMAX.nc`

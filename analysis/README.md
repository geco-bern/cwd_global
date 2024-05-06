# cwd_global: Apply CWD algorithm to global files

## Workflow

1.  `make_tidy_cmip6.R`: Make original global files tidy. Save as nested data frames, where each row is one grid cell and time series is nested in the column `data`. Separate files are written for each longitudinal band. Writes files:

    `~/data/cmip6-ng/tidy/evspsbl_mon_CESM2_historical_r1i1p1f1_native_ilon_<ilon>.rds`

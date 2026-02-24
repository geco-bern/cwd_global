#!/usr/bin/env Rscript
# script is called with one arguments for parallelization:
# 1. year to extract

# Example:
# >./collect_pcwd_results.R 2024

# # When using this script directly from RStudio, not from the shell, specify
# args <- 2022

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==1)

curr_year <- as.integer(args[1])

# options(repos = c(CRAN = "https://cloud.r-project.org"))
# install.packages(c("map2tidy", "dplyr", "stringr", "purrr", "ncdf4"))
# install.packages(c("gtable", "farver", "RColorBrewer")) # TODO: These trigger errors if missing when library(rgeco)
# devtools::install_github("geco-bern/rgeco")
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

library(dplyr)
library(map2tidy)
library(multidplyr)
library(abind)

indir        <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd"
outfile_pcwd <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_netcdf/data_derived_03_daily_pcwd_YYYY_r-generated.nc" # adjust path to where the file should be written to

# 3600 LON slices (for 1 year) use (`seff xxxxxx_1962`) XXGB memory  runtime Xmin    output NetCDF: XXMB
# 1000 LON slices (for 1 year) use (`seff xxxxxx_1962`) XXGB memory  runtime Xmin    output NetCDF: XXMB
# 100  LON slices (for 1 year) use (`seff 46214913_1962 and other try xxxxxx_1962`) 14GB memory  runtime 57min    output NetCDF: XXMB
# 10   LON slices (for 1 year) use (`seff 46213417_1962`) 13GB memory  runtime  6min    output NetCDF: 26MB

dir.create(dirname(outfile_pcwd), showWarnings = FALSE)

# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd <- list.files(indir, pattern = "ERA5Land_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)

# 2) Function to Process one file --------------------------------------------------------
# For development: Use Bern coordinates 46.947687535597794, 7.441952632324079
# basename(filnams_pcwd[75])
# rds_name <- filnams_pcwd[75]
subset_pcwd_for_year_to_nested_dataframe <- function(rds_name, curr_year_arg){
  start <- Sys.time()
  full_tidy_slice <- readRDS(rds_name)
  print(sprintf("%10.0f secs: to read      %s", Sys.time() - start, rds_name))
  flush.console()

  start <- Sys.time()
  out <- full_tidy_slice |>
    # dplyr::filter(lat > 46, lat < 47) |> # slice(1) |>    # TODO: for development we also subset lat to be between 46 and 47 North
    # pcwd generated nested lists with elements 'inst' and 'df'. We only use df
    tidyr::unnest_wider(data) |> select(-inst) |> select(lon, lat, df) |>
    tidyr::unnest(df) |>
    # subset year
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::filter(year == !!curr_year_arg) |>
    select(lon, lat, date, pcwd_mm = deficit)  |>
    tidyr::nest(year_timeseries = c(date, pcwd_mm)) |>
    tidyr::nest(lat_cubes = c(lat, year_timeseries))
  print(sprintf("%10.0f secs: to process 1 %s", Sys.time() - start, rds_name))
  flush.console()

  # explicitly drop large intermediates and trigger GC to reduce RAM usage
  rm(full_tidy_slice)
  gc(FALSE)

  out
}

## 3) Process files --------------------------------------------------------
nested_df_pcwd <- lapply(filnams_pcwd,
  function(filnam) subset_pcwd_for_year_to_nested_dataframe(filnam, curr_year_arg = curr_year)
  ) |> bind_rows()

## 4) Output to global NetCDF file ---------------------------------
##    This is required as NetCDF is a gridded format.
##    We need to reshape the 2D data.frame as N-D gridded array.
##    NOTE: this assumes that the tidy data are available on a regular grid.
##    NOTE: this does assume that the tidy data are ordered.
##
make_netcdf_gridded_array <- function(df_nested, varname){

  start <- Sys.time()
  # Get coordinates of regular grid (assumes regular grid)
  lons = df_nested$lon
  lat  = df_nested$lat_cubes[[1]]$lat                       # NOTE: use 1st element assume all following have same lat (regular grid)
  dates= df_nested$lat_cubes[[1]]$year_timeseries[[1]]$date # NOTE: use 1st element assume all following have same lat (regular grid)

  # Make 3D - array
  arr <- df_nested |>
    dplyr::select(-lon) |> tidyr::unnest(lat_cubes) |>
    dplyr::select(-lat) |> tidyr::unnest(year_timeseries) |>
    dplyr::select(-date) |> magrittr::extract2("pcwd_mm") |>
    # bring into array form
    array(dim      = c(length(lons), length(lat), length(dates)),
          dimnames = list("lon"=lons, "lat"=lat,  "date"=dates))

  # Now arrange a slice vertically with row=lat, col=lon, nmat=date (North-East-Date)
  arr_NE_date <- aperm(arr, c(2,1,3))

  # create object for use in rgeco::write_nc2()
  obj <- list(
    lon  = lons,
    lat  = lat,
    time = dates,
    vars = setNames(list( c(arr_NE_date) ),
                    varname)
  )

  print(sprintf("%10.0f secs: to regrid array: %s", Sys.time() - start, varname))
  flush.console()

  return(obj)
}

obj_pcwd <- make_netcdf_gridded_array(nested_df_pcwd, varname="pcwd_mm")
# str(obj_pcwd)



# Write NetCDF file:
log_str <- sprintf(
    "Created on: %s, processing input data from: %s",
    Sys.Date(), indir)

rgeco::write_nc2(
  obj_pcwd,
  varnams     = "pcwd_mm",
  make_tdim   = TRUE,
  path        = gsub("YYYY", curr_year, outfile_pcwd),
  units_time  = "days since 2001-01-01",
  att_title   = "Potential Cumulative Water Deficit for ERA5Land data",
  att_history = log_str
)

# Check output:
# tidync::tidync(gsub("YYYY", curr_year, outfile_pcwd))


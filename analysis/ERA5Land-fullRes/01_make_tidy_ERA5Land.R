#!/usr/bin/env Rscript

# script is called without any arguments

Sys.getpid()

# devtools::install_github("geco-bern/map2tidy")
# remotes::install_github("geco-bern/map2tidy", ref = "fix-lon-hardcoded")
devtools::install_github("geco-bern/map2tidy@v2.1.4")
library(map2tidy)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(ncdf4)
# list demo file path

# adjust path to where your ERA5Land data is located
path_ERA5Land <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
outdir <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy"
dir.create(outdir, recursive = T)

# ncores <- 180
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads


# Precipitation - daily resolution --------------------------------------------------
#select tot_tp files for only 2018, 2019, and 2020
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_tp\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_pr <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_tp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,  #ERA5Land only contains land gridcells; keep NAs for spatial integrity
  outdir = file.path(outdir, "total_prec"),
  fileprefix = "ERA5Land_UTCDaily_tottp",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)
# Check if any unsuccessful:
stopifnot(nrow(res_pr |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Temperature - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_t2m\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_t2m <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_t2m",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = file.path(outdir, "t2m"),
  fileprefix = "ERA5Land_UTCDaily_t2m",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)
# Check if any unsuccessful:
stopifnot(nrow(res_t2m |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Potential evapotranspiration - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_pev\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_pev <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_pev",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = file.path(outdir, "tot_pet"),
  fileprefix = "ERA5Land_UTCDaily_totpev",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)


# Surface Pressure - daily resolution -------------------------------
# Only list files that start with "monthly_" and end with ".nc"
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_sp\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_pev <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_sp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = file.path(outdir, "mean_sp"),
  fileprefix = "ERA5Land_UTCDaily_sp",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)
# Check if any unsuccessful:
stopifnot(nrow(res_pev |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Computed Netradiation - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_netrad <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "netrad",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = file.path(outdir, "netrad"),
  fileprefix = "ERA5Land_UTCDaily_netrad",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)
# Check if any unsuccessful:
stopifnot(nrow(res_netrad |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Surface solar radiation downwards (ssrd) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_ssrd\\.(2018|2019|2020)\\.nc$",   # TODO: change here to include all years
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_ssrd <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_ssrd",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = file.path(outdir, "ssrd"),
  fileprefix = "ERA5Land_UTCDaily_ssrd",
  ncores = ncores,
  overwrite = FALSE,
  filter_lon_between_degrees = c(0,2) # TODO: remove this
)
# Check if any unsuccessful:
stopifnot(nrow(res_ssrd |> unnest(data) |> filter(!grepl("Written", data))) == 0)

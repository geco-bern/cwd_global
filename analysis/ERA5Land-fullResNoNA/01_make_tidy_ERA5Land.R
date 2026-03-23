#!/usr/bin/env Rscript

# script is called without any arguments
Sys.getpid()

# When reloading re-install packages from renv.lock by doing:
renv::restore()

# # When setting up we installed packages and recorded them into renv.lock:
# # also see: https://www.daryavanichkina.com/posts/210728_renvhpc.html
# install.packages(c("renv", "pak","here"))
# library(renv); options(renv.config.pak.enabled = TRUE); options("renv.config.pak.enabled")
# library(here)
# setwd("../cwd_global/analysis/ERA5Land-fullResNoNA")
# setwd("../../../cwd_global/analysis/ERA5Land-fullResNoNA") # make a RProj in analysis subfolder for using analysis-specific renv
# setwd(here::here("analysis/ERA5Land-fullResNoNA"))
# renv::init(project = here::here("analysis/ERA5Land-fullResNoNA"),
#            repos = c(CRAN = "https://cloud.r-project.org"),
#            bare = TRUE)
# options(renv.config.pak.enabled = TRUE); options("renv.config.pak.enabled")
# renv::install(c("dplyr", "stringr", "purrr", "ncdf4", "tidyr", "readr"))
# renv::install("geco-bern/map2tidy@v2.1.4")
# renv::install("geco-bern/cwd")
# renv::install("geco-bern/rgeco") # for creation of NetCDF
# renv::status()
# renv::snapshot()
# renv::status()

library(map2tidy)
library(dplyr)
library(stringr)
library(purrr)
library(ncdf4)
# list demo file path

# adjust path to where your ERA5Land data is located
path_ERA5Land <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_00_dailyUTC_v3/" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
outdir        <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy.narm_dailyUTC_v3/tidy1950-2024"
dir.create(outdir, recursive = T)

    # What would be available at the input path:
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.9G May 24  2025 ERA5Land_UTCDaily.tot_ssr.2024.nc 
    # -rw-rw----  1 fb24k097 cs_occr_geco 2.2G May 24  2025 ERA5Land_UTCDaily.tot_str.2024.nc
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.5G May 24  2025 ERA5Land_UTCDaily.tot_tp.2024.nc
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.8G May 24  2025 ERA5Land_UTCDaily.mean_sp.2024.nc
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.8G May 24  2025 ERA5Land_UTCDaily.mean_t2m.2024.nc
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.8G May 24  2025 ERA5Land_UTCDaily.tot_ssrd.2024.nc  # unused here
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.9G May 24  2025 ERA5Land_UTCDaily.tot_pev.2024.nc   # unused here
    # -rw-rw----  1 fb24k097 cs_occr_geco 2.3G May 24  2025 ERA5Land_UTCDaily.mean_wind10.2024.nc # unused here
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.8G May 24  2025 ERA5Land_UTCDaily.mean_d2m.2024.nc # unused here
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.4G May 24  2025 ERA5Land_UTCDaily.min_t2m.2024.nc # unused here
    # -rw-rw----  1 fb24k097 cs_occr_geco 1.4G May 24  2025 ERA5Land_UTCDaily.max_t2m.2024.nc # unused here

# ncores <- 180
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads


# Surface net solar radiation (ssr) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_ssr\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_tot_ssr <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_ssr",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "tot_ssr"),
  fileprefix = "ERA5Land_UTCDaily_tot_ssr",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_ssr |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Surface net thermal radiation (str) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_str\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_tot_str <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_str",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "tot_str"),
  fileprefix = "ERA5Land_UTCDaily_tot_str",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_str |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Total precipitation (tp) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_tp\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_tot_tp <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_tp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "tot_tp"),
  fileprefix = "ERA5Land_UTCDaily_tot_tp",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_tp |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Surface Pressure - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_sp\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_mean_sp <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_sp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "mean_sp"),
  fileprefix = "ERA5Land_UTCDaily_mean_sp",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_str |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Temperature - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_t2m\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_mean_t2m <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_t2m",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "mean_t2m"),
  fileprefix = "ERA5Land_UTCDaily_mean_t2m",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_str |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Potential evaporation (pev) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_pev\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_tot_pev <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_pev",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "tot_pev"),
  fileprefix = "ERA5Land_UTCDaily_tot_pev",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_ssrd |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)


# Surface solar radiation downward (ssrd) - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_ssrd\\.[0-9]{4}\\.nc$",
  full.names = TRUE
)
# check files and naming of dimensions etc...
tidync::tidync(filnam[[1]])
# Convert to tidy
res_tot_ssrd <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_ssrd",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = TRUE,  #remove NAs for efficiency. For spatial integrity, creating NetCDF requires manual grid specification
  outdir = file.path(outdir, "tot_ssrd"),
  fileprefix = "ERA5Land_UTCDaily_tot_ssrd",
  ncores = ncores,
  overwrite = FALSE
)
# Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
# stopifnot(nrow(res_tot_ssrd |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)

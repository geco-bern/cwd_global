#!/usr/bin/env Rscript

# script is called without any arguments

Sys.getpid()

library(renv)
source("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land-lowRes/renv/activate.R") # somewhat on UBELIX this needs to be done manually sometimes
renv::project()
options(renv.config.pak.enabled = FALSE)
renv::restore(prompt = FALSE)
# If errors occur. First ensure you run exactly the same hardware and software (modules) as in 00_setup_R.R
# If error persists try to run the following command in R for the correpsonding package:
#    renv::rebuild(packages="stringfish")
#    renv::rebuild(packages="map2tidy")
#    renv::rebuild("qs2") # or alternatively: renv::rebuild(c("RcppParallel", "qs2"))

library(map2tidy)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(ncdf4)

# # list demo file path
# # adjust path to where your ERA5Land data is located
# # path_ERA5Land <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded"

# cons uses conservative regridding for tp and t2m
#path_ERA5Land      <- "/storage/capacity/occr_geco/data_2/archive_projects/MSc_2025_phelpap_cwdModESim/ERA5Land_1950-2024/ERA5Land_regridded"      #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
#path_ERA5Land_cons <- "/storage/capacity/occr_geco/data_2/archive_projects/MSc_2025_phelpap_cwdModESim/ERA5Land_1950-2024_cons/ERA5Land_regridded" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
path_ERA5Land      <- "/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_2025-2026_regridded_1.875deg" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
path_ERA5Land_cons <- "/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_2025-2026_regridded_1.875deg" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon

outdir <- "/storage/scratch/giub_geco/fbernhard/ERA5Land-lowRes_2025-2026/tidy"

# Precipitation - daily resolution --------------------------------------------------
#varnam <- "Daily_tp_pev" # TODO: from previous version
varnam <- "Daily_tp"
filnam <- list.files(
  paste0(path_ERA5Land_cons, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir <- file.path(outdir, "tot_tp")

# Convert to tidy
res_pr <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_tp",
  lonnam = "lon",
  latnam = "lat",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,  #ERA5Land only contains land gridcells; keep NAs for spatial integrity
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_tot_tp",
  ncores = 8,
  overwrite = FALSE
)

# # Check if any unsuccessful:
# stopifnot(nrow(res_pr |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Temperature - daily resolution -------------------------------
varnam <- "Daily_t2m"
filnam <- list.files(
  paste0(path_ERA5Land_cons, "/", varnam),
  pattern = ".nc", 
  # pattern = ".*mean_t2m\\.202[56]\\.nc$", # NOTE: subsetting only certain years TODO: activate for other variables
  full.names = TRUE)
output_dir <- file.path(outdir, "mean_t2m")

# Convert to tidy
res_t2m <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_t2m",
  lonnam = "lon",
  latnam = "lat",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_mean_t2m",
  ncores = 8,
  overwrite = FALSE
)

# # Check if any unsuccessful:
# stopifnot(nrow(res_t2m |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Surface Pressure - monthly resolution -------------------------------
varnam <- "Monthly_sp"
# Only list files that start with "monthly_" and end with ".nc"
filnam <- list.files(
  file.path(path_ERA5Land, varnam),
  pattern = "^monthly_.*\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "mean_sp")

# Convert to tidy
res_sp <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_sp",
  lonnam = "lon",
  latnam = "lat",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_mean_sp",
  ncores = 8,
  overwrite = FALSE
)

# # Check if any unsuccessful:
# stopifnot(nrow(res_sp |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Computed Netradiation - monthly resolution -------------------------------
varnam <- "Monthly_netrad"
filnam <- list.files(
  paste0(path_ERA5Land, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir <- file.path(outdir, "netrad")

# NOTE: preprocessing transformed tot_ssr and tot_str into netrad,
#       took monthly mean and transformed the units

# Convert to tidy
res_netrad <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "netrad",
  lonnam = "lon",
  latnam = "lat",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_netrad",
  ncores = 8,
  overwrite = FALSE
)

# # Check if any unsuccessful:
# stopifnot(nrow(res_netrad |> unnest(data) |> filter(!grepl("Written", data))) == 0)


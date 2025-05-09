#!/usr/bin/env Rscript

# script is called without any arguments

Sys.getpid()

library(map2tidy)
devtools::load_all("~/map2tidy/R/map2tidy.R")
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(ncdf4)

# list demo file path
# adjust path to where your ERA5Land data is located
path_ERA5Land <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
outdir <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy"

##### Original tidy workflow without NA filling:

# # Precipitation - daily resolution --------------------------------------------------
# varnam <- "Daily_tp_pev"
# filnam <- list.files(
#   paste0(path_ERA5Land, "/", varnam),
#   pattern = ".nc", full.names = TRUE)
# output_dir <- file.path(outdir, "total_prec")
#
# #filnam <- filnam[19:75]
#
# # Convert to tidy
# res_pr <- map2tidy:::map2tidy(
#   nclist = filnam,
#   varnam = "tot_tp",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "valid_time",
#   do_chunks = TRUE,
#   na.rm = FALSE,
#   outdir = output_dir,
#   fileprefix = "ERA5Land_UTCDaily_tottp",
#   ncores = 1,
#   overwrite = FALSE
# )
#
# # # For each nested data frame, complete the grid to include all expected latitudes.
# # # Missing tot_tp values will be filled with NA.
# # res_pr <- res_pr %>%
# #   mutate(data = map(data, ~ .x %>%
# #                       complete(lon, valid_time, lat = expected_lat,
# #                                fill = list(tot_tp = NA))))
#
# # Check if any unsuccessful:
# stopifnot(nrow(res_pr |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# # Potential Evapotranspiration - monthly resolution -------------------------------
# varnam <- "tot_pev"
# filnam <- list.files(
#   paste0(path_ERA5Land),
#   pattern = ".nc", full.names = TRUE)
# output_dir <- file.path(outdir, "total_pet")
#
# # Convert to tidy
# res_pev <- map2tidy:::map2tidy(
#   nclist = filnam,
#   varnam = "tot_pev",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "valid_time",
#   do_chunks = TRUE,
#   na.rm = FALSE,
#   outdir = output_dir,
#   fileprefix = "ERA5Land_UTCDaily_totpev",
#   ncores = 1,
#   overwrite = FALSE
# )
#
# # # Complete the grid for tot_pev as well
# # res_pev <- res_pev %>%
# #   mutate(data = map(data, ~ .x %>%
# #                       complete(lon, valid_time, lat = expected_lat,
# #                                fill = list(tot_pev = NA))))
#
# # Check if any unsuccessful:
# stopifnot(nrow(res_pev |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Temperature - daily resolution -------------------------------
varnam <- "Daily_t2m"
filnam <- list.files(
  paste0(path_ERA5Land, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir <- file.path(outdir, "t2m")

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
  fileprefix = "ERA5Land_UTCDaily_t2m",
  ncores = 1,
  overwrite = FALSE
)

# # Complete the grid for tot_pev as well
# res_pev <- res_pev %>%
#   mutate(data = map(data, ~ .x %>%
#                       complete(lon, valid_time, lat = expected_lat,
#                                fill = list(tot_pev = NA))))

# Check if any unsuccessful:
stopifnot(nrow(res_t2m |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Surface Pressure - monthly resolution -------------------------------
varnam <- "Monthly_mean_sp"
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
  fileprefix = "ERA5Land_UTCDaily_sp",
  ncores = 1,
  overwrite = FALSE
)

# # Complete the grid for tot_pev as well
# res_pev <- res_pev %>%
#   mutate(data = map(data, ~ .x %>%
#                       complete(lon, valid_time, lat = expected_lat,
#                                fill = list(tot_pev = NA))))

# Check if any unsuccessful:
stopifnot(nrow(res_sp |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Computed Netradiation - monthly resolution -------------------------------
varnam <- "Monthly_netrad"
filnam <- list.files(
  paste0(path_ERA5Land, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir <- file.path(outdir, "netrad")

# filnam = filnam[1]
# test <- nc_open(filnam)
# netrad_test <- ncvar_get(test, varid="netrad")
# nc_close(test)



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
  ncores = 1,
  overwrite = FALSE
)

# # Complete the grid for tot_pev as well
# res_pev <- res_pev %>%
#   mutate(data = map(data, ~ .x %>%
#                       complete(lon, valid_time, lat = expected_lat,
#                                fill = list(tot_pev = NA))))

# Check if any unsuccessful:
stopifnot(nrow(res_netrad |> unnest(data) |> filter(!grepl("Written", data))) == 0)


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
"/scratch/"
# list demo file path

# adjust path to where your ERA5Land data is located
path_ERA5Land <- "/storage/scratch/giub_geco/yelmejjaouy/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
outdir <- "/storage/scratch/giub_geco/yelmejjaouy/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy"


# Precipitation - daily resolution --------------------------------------------------
#varnam <- "Daily.tot_pev" this is a subdfolder in Patricia's data directory
#the variable name is tot_tp

#select tot_tp files for only 2018, 2019, and 2020
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_tp\\.(2018|2019|2020)\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "total_prec")

# check files
length(outdir)
head(outdir)
#rename the longitude and latitude in the nc

# x1 <- filnam[[1]]
# x <- tidync::tidync(x1)
# ncdf_available_dims <- tidync::hyper_dims(x)
# lonnam = "longitude"
# x$transforms[[lonnam]] |>
#   dplyr::select(all_of(c(lon_index = 'index', lon_value = lonnam)))

# Convert to tidy
res_pr <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_tp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,  #ERA5Land only contains land gridcells; keep NAs for spatial integrity
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_tottp",
  ncores = 32,
  overwrite = FALSE
)


nc_file <-
  library(ncdf4)
nc <- nc_open(filnam[1])
names(nc$dim)
nc_close(nc)

# Check if any unsuccessful:
stopifnot(nrow(res_pr |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Temperature - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_t2m\\.(2018|2019|2020)\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "t2m")

# Convert to tidy
res_t2m <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_t2m",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_t2m",
  ncores = 40,
  overwrite = FALSE
)

# Check if any unsuccessful:
stopifnot(nrow(res_t2m |> unnest(data) |> filter(!grepl("Written", data))) == 0)

filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_pev\\.(2018|2019|2020)\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "tot_pet")

# Convert to tidy
res_pev <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_pev",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_totpev",
  ncores = 40,
  overwrite = FALSE
)


# Surface Pressure - monthly resolution -------------------------------
# Only list files that start with "monthly_" and end with ".nc"

filnam <- list.files(
  path_ERA5Land,
  pattern = ".*mean_sp\\.(2018|2019|2020)\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "mean_sp")

# Convert to tidy
res_pev <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "mean_sp",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_sp",
  ncores = 40,
  overwrite = FALSE
)


# Check if any unsuccessful:
stopifnot(nrow(res_sp |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Computed Netradiation - monthly resolution -------------------------------
# varnam <- "Monthly_netrad"
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*\\.(2018|2019|2020)\\.nc$", full.names = TRUE)
output_dir <- file.path(outdir, "netrad")

# Convert to tidy
res_netrad <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "netrad",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_netrad",
  ncores = 40,
  overwrite = FALSE
)

# Check if any unsuccessful:
stopifnot(nrow(res_netrad |> unnest(data) |> filter(!grepl("Written", data))) == 0)


# ssrd - daily resolution -------------------------------
filnam <- list.files(
  path_ERA5Land,
  pattern = ".*tot_ssrd\\.(2018|2019|2020)\\.nc$",
  full.names = TRUE
)
output_dir <- file.path(outdir, "ssrd")

# Convert to tidy
res_ssrd <- map2tidy:::map2tidy(
  nclist = filnam,
  varnam = "tot_ssrd",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "valid_time",
  do_chunks = TRUE,
  na.rm = FALSE,
  outdir = output_dir,
  fileprefix = "ERA5Land_UTCDaily_ssrd",
  ncores = 60,
  overwrite = FALSE
)

# Check if any unsuccessful:
stopifnot(nrow(res_t2m |> unnest(data) |> filter(!grepl("Written", data))) == 0)



#!/usr/bin/env Rscript

# script is called with two arguments for parallelization:
# 1. counter for chunks (e.g. # of each compute node)
# 2. total number of chunks (e.g. number of total compute nodes)

# Note that these arguments can be used to distribute over multiple nodes.
# Distribution over CPU cores of a single node is handled by multidplyr
# and argument ncores inside of this script.

# Example for 4 CPU-nodes:
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 2 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 3 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 4 4

# Example for 1 CPU-nodes:
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 1
# # When using this script directly from RStudio, not from the shell, specify
# args <- c(1, 180)
# args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)
print(sprintf("%s: Starting 02_apply_pcwd_global_ERA5Land_ubelix.R with arguments: %s",
              Sys.time(),
              paste0(args, collapse = " ")))

# When reloading re-install packages from renv.lock by doing:
renv::restore()

library(lubridate)
library(dplyr)
library(map2tidy)
library(multidplyr)
# library(terra)
library(tidyr)
library(cwd)       # using cwd: v2.0 or newer       # 1. Load cwd package first
library(parallelly)

# Working directory (see main.sh): /storage/homefs/fb24k097/GitHub/geco-bern/cwd_global/analysis/ERA5Land-fullRes
source("../../R/ERA5Land-fullResNoNA/ERA5Land_compute_pcwd_byLON.R")         # 4. And another wrapper that loads the tidied RDS files
source("../../R/ERA5Land-fullResNoNA/get_cwd_withSnow_and_reset_ERA5Land.R") # 3. Then source the wrapper that uses it
source("../../R/ERA5Land-fullResNoNA/ERA5Land_simulate_snow.R")              # 2. Source ERA5 simulate snow first

indir  <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy.narm_dailyUTC_v3/tidy1950-2024/"
outdir <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd.narm_v2-doy-reset"
dir.create(outdir, showWarnings = FALSE)


# 1) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"

filnams <- list.files(file.path(indir, "tot_tp"),  # use tot_tp folder to get all LON
                      pattern = infile_pattern, full.names = TRUE)
if (length(filnams) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}


# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by
#     another compute node)
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for compute node
  as.integer(args[2]),  # total number of compute node
  length(filnams)       # total number of longitude indices
)

# 2b) Parallelize job across cores on a single node
# ncores <- 1 # start small
# ncores <- 180
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads

cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster, sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "lubridate",
                                "cwd",
                                # "rpmodel", # TODO: remove this if not needed
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                               = indir,
    outdir                              = outdir,
    ERA5Land_compute_pcwd_byLON         = ERA5Land_compute_pcwd_byLON,        # make the function known for each core
    get_cwd_withSnow_and_reset_ERA5Land = get_cwd_withSnow_and_reset_ERA5Land # make the function known for each core
  )
# FOR DEVELOPMENT:
# LON_string <- gsub(".rds","", gsub("^.*(LON_)", "\\1", filnams[2]))
# # debug(get_cwd_withSnow_and_reset_ERA5Land)
# # debug(ERA5Land_compute_pcwd_byLON)
# ERA5Land_compute_pcwd_byLON(LON_string, indir, outdir)


# 3) Process files --------------------------------------------------------
#    If some are missing from the 3600 simply rerun this again manually. It should filter out already processed files.
out <- tibble(in_fname = filnams[vec_index]) |>
  mutate(LON_string = gsub("^.*?(LON_[0-9.+-]*).rds$", "\\1", basename(in_fname))) |>

  # Define the corresponding output filename based on how your output files are named
  # Assuming output files follow the same LON_string format with .rds extension
  mutate(out_fname = file.path(outdir, paste0("ERA5Land_pcwd_",LON_string, ".rds"))) |>

  # Filter out files that already have corresponding output files
  filter(!file.exists(out_fname)) |>
  # Remove unnecessary columns if needed
  dplyr::select(-in_fname, -out_fname) |>

  multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    LON_string,
    ~ERA5Land_compute_pcwd_byLON(
      .,
      indir           = indir,
      outdir          = outdir,
      reduce_rds_size = TRUE))
  ) |> collect()

print(out)

print(out |> unnest(out) |> filter(grepl("Failed", out)))

# CHECK:
# file.info("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd.narm_v2-doy-reset/ERA5Land_pcwd_LON_+007.600.rdsallyears_onlypcwd_mm.rds") |> tibble()
# readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd.narm_v2-doy-reset/ERA5Land_pcwd_LON_+007.600.rdsallyears_onlypcwd_mm.rds")
# # A tibble: 491,290 × 5
#      lon   lat date       pcwd_mm  year
#    <dbl> <dbl> <date>       <dbl> <dbl>
#  1  7.60   -90 2021-01-01  0       2021
#  2  7.60   -90 2021-01-02  0       2021
#  3  7.60   -90 2021-01-03  0       2021

### COMPARE WITH EARLIER ATTEMPTS
# earlier attempts with NA coordinates included:
# file.info("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd_v2-doy-reset/ERA5Land_pcwd_LON_+007.600.rdsallyears_onlypcwd_mm.rds") |> tibble()
# readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd_v2-doy-reset/ERA5Land_pcwd_LON_+007.600.rdsallyears_onlypcwd_mm.rds")
# # A tibble: 49,336,594 × 5                 !!! 100x more coordinates
#      lon   lat date       pcwd_mm  year
#    <dbl> <dbl> <date>       <dbl> <dbl>
#  1  7.60   -90 1950-01-01  0.149   1950
#  2  7.60   -90 1950-01-02  0.269   1950
#  3  7.60   -90 1950-01-03  0.383   1950

# earlier attempts with NA coordinates included and with instances:
# file.info("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+007.600.rds") |> tibble()
# readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+007.600.rds")
# # A tibble: 1,801 × 3                    !!! all 1800 coordinates are included
#      lon   lat data
#    <dbl> <dbl> <list>
#  1  7.60 -90   <named list [2]>
#  2  7.60 -89.9 <named list [2]>
### COMPARE WITH EARLIER ATTEMPTS


## RERUN SOME MISSING FILES MANUALLY:
# # coord_to_rerun <- c(272.000,275.000,275.500,276.000,287.300,289.200,282.600,285.800,291.100,295.700)
# # coord_to_rerun <- c(295.700)
# for (curr_coord in coord_to_rerun){
#   print(curr_coord)
#   res_mean_sp <- map2tidy:::map2tidy(
#     nclist = filnam,
#     varnam = "mean_sp",
#     lonnam = "longitude",
#     latnam = "latitude",
#     timenam = "valid_time",
#     do_chunks = TRUE,
#     na.rm = FALSE,
#     outdir = file.path(outdir, "mean_sp"),
#     fileprefix = "ERA5Land_UTCDaily_mean_sp",
#     ncores = ncores,
#     overwrite = FALSE, filter_lon_between_degrees = curr_coord + c(-0.05, 0.05)
#   )
#   # Check if any unsuccessful (outcommented since this can error if overwrite = FALSE and previous present):
#   # stopifnot(nrow(res_mean_sp |> unnest(data) |> filter(!grepl("(Written)|(File exists)", data))) == 0)
# }
#
#
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+272.000.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+275.000.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+275.500.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+276.000.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+287.300.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+289.200.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+282.600.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+285.800.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+291.100.rds")
# # readRDS("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_01_tidy_dailyUTC_v3/tidy1950-2024/mean_sp/ERA5Land_UTCDaily_mean_sp_LON_+295.700.rds")

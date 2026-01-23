#!/usr/bin/env Rscript

# script is called with two arguments for parallelization:
# 1. counter for chunks (e.g. # of each compute node)
# 2. total number of chunks (e.g. number of total compute nodes)

# Note that these arguments can be used to distribute over multiple nodes.
# Distribution over CPU cores of a single node is handled by multidplyr
# and argument ncores inside of this script.

# Example for 4 CPU-nodes:
# >./apply_cwd_global.R 1 4
# >./apply_cwd_global.R 2 4
# >./apply_cwd_global.R 3 4
# >./apply_cwd_global.R 4 4

# Example for 1 CPU-nodes:
# >./apply_cwd_global.R 1 1
# # When using this script directly from RStudio, not from the shell, specify
# args <- c(1, 180)
# args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)

options(repos = c(CRAN = "https://cloud.r-project.org"))
# install.packages(c("map2tidy", "dplyr", "stringr", "purrr", "ncdf4"))
# install.packages(c("lubridate"))
# install.packages(c("tidyr"))
# install.packages(c("ncdf4"))
# install.packages(c("readr"))
remotes::install_github("geco-bern/cwd")

library(lubridate)
library(dplyr)
library(map2tidy)
library(multidplyr)
library(terra)
library(tidyr)
library(cwd)       # 1. Load cwd package first
library(parallelly)

# Working directory (see main.sh): /storage/homefs/fb24k097/GitHub/geco-bern/cwd_global/analysis/ERA5Land-fullRes
source("../../R/ERA5Land-fullRes/ERA5Land_compute_pcwd_byLON.R")         # 4. And another wrapper that loads the tidied RDS files
source("../../R/ERA5Land-fullRes/get_cwd_withSnow_and_reset_ERA5Land.R") # 3. Then source the wrapper that uses it
source("../../R/ERA5Land-fullRes/ERA5Land_simulate_snow.R")              # 2. Source ERA5 simulate snow first

indir  <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy1950-2024/"
# indir  <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/01_daily_tidy1950-2024/" # TODO: switch to this
outdir <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/02_daily_pcwd"
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
                                "rpmodel",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                               = indir,
    outdir                              = outdir,
    ERA5Land_compute_pcwd_byLON         = ERA5Land_compute_pcwd_byLON,        # make the function known for each core
    get_cwd_withSnow_and_reset_ERA5Land = get_cwd_withSnow_and_reset_ERA5Land # make the function known for each core
  )
# FOR DEVELOPMENT:
# LON_string <- gsub(".rds","", gsub("^.*(LON_)", "\\1", filnams[2]))
# debug(get_cwd_withSnow_and_reset_ERA5Land)
# debug(ERA5Land_compute_pcwd_byLON)
# ERA5Land_compute_pcwd_byLON(LON_string, indir, outdir)


# 3) Process files --------------------------------------------------------

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
      outdir          = outdir))
  ) |> collect()


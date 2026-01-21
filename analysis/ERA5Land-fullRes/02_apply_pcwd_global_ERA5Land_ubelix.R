#!/usr/bin/env Rscript

# script is called with two arguments for parallelization:
# 1. counter for chunks (e.g. # of each compute node)
# 2. total number of chunks (e.g. number of total compute nodes)

# Note that these arguments can be used to distribute over multiple nodes.
# Distribution over CPU cores of a single node is handled by multidplyr
# and argument ncores in the script.

# Example for 4 CPU-nodes:
# >./apply_cwd_global.R 1 4
# >./apply_cwd_global.R 2 4
# >./apply_cwd_global.R 3 4
# >./apply_cwd_global.R 4 4

# Example for 1 CPU-nodes:
# >./apply_cwd_global.R 1 1
# # When using this script directly from RStudio, not from the shell, specify
args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)

options(repos = c(CRAN = "https://cloud.r-project.org"))
remotes::install_github("geco-bern/cwd")
remotes::install_github("geco-bern/rgeco")
remotes::install_github("geco-bern/rpmodel")

library(dplyr)
library(map2tidy)
library(multidplyr)
library(terra)
library(tidyr)
library(cwd)
# devtools::load_all("~/cwd/R/cwd.R")
library(rgeco)
library(rpmodel)
library(parallelly)
# devtools::load_all("~/cwd")

# source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))
source(here::here("R/ERA5Land-fullRes/ERA5Land_compute_pcwd_byLON.R"))
source(here::here("R/ERA5Land-fullRes/get_cwd_withSnow_and_reset_ERA5Land.R"))
source(here::here("R/ERA5Land-fullRes/ERA5_simulate_snow.R"))

indir  <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy1950-2024/"
outdir <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/02_pcwd" # TODO: check this
dir.create(outdir, showWarnings = FALSE)

# 1a) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"

filnams <- list.files(file.path(indir, "tot_tp"),  # use precip folder as example; change year
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

# cl <- multidplyr::new_cluster(ncores)
# cl
#
# multidplyr::cluster_library(cl, c(
#   "map2tidy", "dplyr", "purrr", "tidyr", "readr", "here", "rpmodel", "magrittr", "cwd"
# ))
#
# multidplyr::cluster_assign(
#   cl,
#   ERA5Land_compute_pcwd_byLON = ERA5Land_compute_pcwd_byLON
# )
# ERA5Land_fullRes_compute_pcwd_byLON(filnams[1], indir, outdir)
# ERA5Land_compute_pcwd_byLON(filnams[1], indir, outdir)
# FOR DEVELOPMENT:
# LON_string <- gsub(".rds","", gsub("^.*(LON_)", "\\1", filnams[1]))
# ERA5Land_compute_pcwd_byLON(LON_string, indir, outdir)

# 1. Load cwd package first
# devtools::load_all("~/cwd")  # if needed

# 2. Source ERA5 simulate snow first
# source("/storage/homefs/ye23g660/Era5_pcwd/cwd_global/R/ERA5Land-fullRes/ERA5_simulate_snow.R")

# 3. Then source the wrapper that uses it
# source("/storage/homefs/ye23g660/Era5_pcwd/cwd_global/R/ERA5Land-fullRes/get_cwd_withSnow_and_reset_ERA5Land.R")
# Load cwd package
# library(cwd)

cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster, sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "here",
                                "cwd",
                                "rpmodel",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                       = indir,
    outdir                      = outdir,
    ERA5Land_compute_pcwd_byLON = ERA5Land_compute_pcwd_byLON   # make the function known for each core
  )
# FOR DEVELOPMENT:
# LON_string <- gsub(".rds","", gsub("^.*(LON_)", "\\1", filnams[1]))
# ERA5Land_compute_pcwd_byLON(LON_string, indir, outdir)


# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk

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


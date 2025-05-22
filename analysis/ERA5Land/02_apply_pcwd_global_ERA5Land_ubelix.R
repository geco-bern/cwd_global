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
#args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)

library(dplyr)
library(map2tidy)
library(multidplyr)
library(terra)
library(tidyr)
library(cwd)
devtools::load_all("~/cwd/R/cwd.R")
library(rgeco)
library(rpmodel)
library(parallelly)

setwd("/storage")
# source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))
source("/storage/homefs/ph23v078/cwd_global/R/ERA5Land_compute_pcwd_byLON.R")
#paste0(here::here(),

indir  <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy"
outdir <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/02_pcwd"
dir.create(outdir, showWarnings = FALSE)

# 1a) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"

filnams <- list.files(file.path(indir, "total_prec"),  # use precip folder as example; change year
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
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads

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
    indir                              = indir,
    outdir                             = outdir,
    ERA5Land_compute_pcwd_byLON = ERA5Land_compute_pcwd_byLON   # make the function known for each core
  )

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



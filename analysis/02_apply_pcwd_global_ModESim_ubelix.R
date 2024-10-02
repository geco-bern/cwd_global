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
library(rgeco)
library(rpmodel)
library(parallelly)

setwd("~/cwd_global")
# source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))
source("~/cwd_global/R/ModESim_compute_pcwd_byLON.R")
#paste0(here::here(),

indir  <- "~/ModESim/tidy/"
outdir <- "~/ModESim/tidy/02_pcwd"
dir.create(outdir, showWarnings = FALSE)

# 1a) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"
# outfile_pattern <- "CWD_result_[LONSTRING]_ANNMAX.rds" # must contain [LONSTRING]

filnams <- list.files(file.path(indir, "1420_01_m001_precip"),  # use precip folder as example; change year
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
    ModESim_compute_pcwd_byLON = ModESim_compute_pcwd_byLON   # make the function known for each core
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk

# 3) Process files --------------------------------------------------------
# ModESim_compute_pcwd_byLON(
#   "LON_-120.000",
#   indir           = indir,
#   outdir          = outdir)


out <- tibble(in_fname = filnams[vec_index]) |>
  mutate(LON_string = gsub("^.*?(LON_[0-9.+-]*).rds$", "\\1", basename(in_fname))) |>
  dplyr::select(-in_fname) |>
  # multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    LON_string,
    ~ModESim_compute_pcwd_byLON(
      .,
      indir           = indir,
      outdir          = outdir))
    ) |> collect()


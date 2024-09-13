#!/usr/bin/env Rscript

# script is called with two arguments for parallelization:
# 1. counter for chunks (e.g. # of each compute node)
# 2. total number of chunks (e.g. number of total compute nodes)

# Note that these arguments can be used to distribute over multiple nodes.
# Distribution over CPU cores of a single node is handled by multidplyr
# and argument ncores in the script.

# Example for 4 CPU-nodes:
# >./get_cwd_annmax.R 1 4
# >./get_cwd_annmax.R 2 4
# >./get_cwd_annmax.R 3 4
# >./get_cwd_annmax.R 4 4

# Example for 1 CPU-nodes:
# >./get_cwd_annmax.R 1 1
# # When using this script directly from RStudio, not from the shell, specify
args <- c(1, 1)

# to receive arguments to script from the shell
# args = commandArgs(trailingOnly=TRUE)

library(dplyr)
library(map2tidy)
library(multidplyr)


# adjust the paths of the indirectory and outdirectory to
# where your cwd and pcwd data is
indir   <- "/data_2/scratch/fbernhard/CMIP6/tidy/02_cwd"
outdir  <- "/data_2/scratch/fbernhard/CMIP6/tidy/03_cwd_ANNMAX/"
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# 1a) Define filenames of files to process:  -------------------------------
filnams_cwd  <- list.files(indir, pattern = "CMIP6_cwd_(LON_[0-9.+-]*).rds",  full.names = TRUE)
filnams_pcwd <- list.files(indir, pattern = "CMIP6_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)

if (length(filnams_cwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}
if (length(filnams_pcwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}

# 1b) Define function to apply to each location:  -------------------------------
source(paste0(here::here(), "/R/cwd_annmax_byilon.R"))


# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by another compute node)
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for compute node
  as.integer(args[2]),  # total number of compute node
  length(filnams_cwd)   # total number of longitude indices
)

# 2b) Parallelize job across cores on a single node
ncores <- 40 # parallel::detectCores() # number of cores of parallel threads

cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster by sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "here",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir       = indir,
    indir      = indir,
    outdir      = outdir,
    cwd_annmax_byLON = cwd_annmax_byLON,   # make the function known for each core
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
# 3) Process files --------------------------------------------------------

# Once for cwd
out_cwd <- tibble(in_fname = filnams_cwd[vec_index]) |>
  # multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~cwd_annmax_byLON(
      .,
      outdir          = outdir))
  ) |> collect()

# Once for pcwd
out_pcwd <- tibble(in_fname = filnams_pcwd[vec_index]) |>
  # multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~cwd_annmax_byLON(
      .,
      outdir          = outdir))
  ) |> collect()




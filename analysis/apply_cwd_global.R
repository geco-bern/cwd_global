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
# args = commandArgs(trailingOnly=TRUE)

library(dplyr)
library(map2tidy)
library(multidplyr)

# source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))
source(paste0(here::here(), "/R/cwd_pcwd_byilon_tailored_for_cmip6.R"))

indir  <- "/data_2/scratch/fbernhard/CMIP6/tidy/"
outdir <- "/data_2/scratch/fbernhard/CMIP6/tidy/cwd_reset2/"
dir.create(outdir, showWarnings = FALSE)

print("getting data for longitude indices:")
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for chunks
  as.integer(args[2]),  # total number of chunks
  as.integer(args[3])   # total number of longitude indices
  )


# 2) Setup parallelization ------------------------------------------------
# parallelize job across cores on a single node
ncores <- 40 # parallel::detectCores() # number of cores of parallel threads

cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster, sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "here",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                              = indir,
    outdir                             = outdir,
    cwd_pcwd_byilon_tailored_for_cmip6 = cwd_pcwd_byilon_tailored_for_cmip6   # make the function known for each core
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk

# 3) Process files --------------------------------------------------------
out <- tibble(ilon = vec_index[1]) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~cwd_pcwd_byilon_tailored_for_cmip6(
      .,
      indir           = indir
      outdir          = outdir))
    )

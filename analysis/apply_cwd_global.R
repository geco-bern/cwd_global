#!/usr/bin/env Rscript

# script is called with three arguments:
# 1. counter for chunks
# 2. total number of chunks
# 3. total number of longitude indices

# Example:
# >./apply_cwd_global.R 1 3 360

args = commandArgs(trailingOnly=TRUE)   # to receive arguments to script from the shell

library(dplyr)
library(map2tidy)
library(multidplyr)

source(paste0(here::here(), "/R/cwd_byilon.R"))

print("getting data for longitude indices:")
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for chunks
  as.integer(args[2]),  # total number of chunks
  as.integer(args[3])   # total number of longitude indices
  )

# number of cores of parallel threads
ncores <- 2 # parallel::detectCores()

# parallelize job
# set up the cluster, sending required objects to each core
cl <- multidplyr::new_cluster(ncores) |>
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "here",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    cwd_byilon = cwd_byilon   # make the function known for each core
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
out <- tibble(ilon = vec_index) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~cwd_byilon(
      .,
      indir = "~/data/cmip6-ng/tidy/evspsbl/",
      outdir = "~/data/cmip6-ng/tidy/cwd/"
      ))
    )


# # un-parallel alternative
# out <- tibble(ilon = vec_index) |>
#   dplyr::mutate(out = purrr::map(
#     ilon,
#     ~cwd_byilon(
#       .,
#       indir = "~/data/cmip6-ng/tidy/evspsbl/",
#       outdir = "~/data/cmip6-ng/tidy/cwd/"
#     ))
#   )

#!/usr/bin/env Rscript
#' @export


# script is called with three arguments:
# 1. counter for chunks
# 2. total number of chunks
# 3. total number of longitude indices

# Example:
# >./apply_cwd_global.R 1 3 360

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)

# # When using this script directly from RStudio, not from the shell, specify
nlon <- 289 # set this by hand. corresponds to length of the longitude dimension in original NetCDF files
args <- c(1, 1, nlon)

#' @import dyplr map2tidy multidplyr
library(dplyr)
library(map2tidy)
library(multidplyr)

#' @importFrom here here
source(paste0(here::here(), "/R/cwd_byilon_cmip6.R"))

print("getting data for longitude indices:")
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for chunks
  as.integer(args[2]),  # total number of chunks
  as.integer(args[3])   # total number of longitude indices
  )

# number of cores of parallel threads
ncores <- 40 #2 # parallel::detectCores()

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
      # read in variables
      indir_evspsbl   = "/data_2/scratch/fbernhard/CMIP6/tidy/evspsbl/",
      indir_tas       = "/data_2/scratch/fbernhard/CMIP6/tidy/tas/",
      indir_prec      = "/data_2/scratch/fbernhard/CMIP6/tidy/pr/",
      indir_rlus      = "/data_2/scratch/fbernhard/CMIP6/tidy/rlus/",
      indir_rlds      = "/data_2/scratch/fbernhard/CMIP6/tidy/rlds/",
      indir_rsds      = "/data_2/scratch/fbernhard/CMIP6/tidy/rsds/",
      indir_rsus      = "/data_2/scratch/fbernhard/CMIP6/tidy/rsus/",
      indir_elevation = "/data_2/scratch/fbernhard/CMIP6/tidy/elevation/",
      outdir_cwd      = "/data_2/scratch/fbernhard/CMIP6/tidy/cwd_reset/test/",
      outdir_pcwd     = "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/",
      fileprefix_cwd  = "cwd",
      fileprefix_pcwd = "pcwd"
      ))
    )


# # un-parallel alternative
# out <- tibble(ilon = vec_index) |>
#    dplyr::mutate(out = purrr::map(
#      ilon,
#        ~cwd_byilon(
#          .,
#          indir_evspsbl = "/data_1/CMIP6/tidy/evspsbl/",
#          indir_tas = "/data_1/CMIP6/tidy/tas/",
#          indir_prec = "/data_1/CMIP6/tidy/pr/",
#          indir_rlus = "/data_1/CMIP6/tidy/rlus/",
#          indir_rlds = "/data_1/CMIP6/tidy/rlds/",
#          indir_rsds = "/data_1/CMIP6/tidy/rsds/",
#          indir_rsus = "/data_1/CMIP6/tidy/rsus/",
#          indir_elevation = "/data_1/CMIP6/tidy/elevation/",
#          outdir_cwd = "/data_2/scratch/CMIP6/tidy/cwd/",
#          outdir_pcwd = "/data_2/scratch/CMIP6/tidy/pcwd/",
#          fileprefix_cwd = "cwd",
#          fileprefix_pcwd = "pcwd"
#        ))
#    )

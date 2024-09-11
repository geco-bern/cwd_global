#!/usr/bin/env Rscript

# script is called with three arguments:
# 1. counter for chunks
# 2. total number of chunks
# 3. total number of longitude indices

# Example:
# >./get_cwd_annmax.R 1 3 360

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)

# # When using this script directly from RStudio, not from the shell, specify
nlon <- 288  # set this by hand. corresponds to length of the longitude dimension in original NetCDF files
args <- c(1, 1, nlon)

library(dplyr)
library(map2tidy)
library(multidplyr)

source(paste0(here::here(), "/R/collect_cwd_annmax_byilon.R"))

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
    collect_cwd_annmax_byilon = collect_cwd_annmax_byilon   # make the function known for each core
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
## cwd vec_index
df_pcwd <- tibble(ilon =  vec_index) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~collect_cwd_annmax_byilon(
      .,
      indir = "/data_1/CMIP6/tidy/pcwd_reset/test/",
      fileprefix = "pcwd"
      ))
    ) |>
  collect() |>
  tidyr::unnest(out)

readr::write_rds(
  df_pcwd,
  paste0(
    indir,
    fileprefix,
    "_ANNMAX.rds"
  )
)

## pcwd
df_pcwd <- tibble(ilon =  vec_index) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~collect_cwd_annmax_byilon(
      .,
      indir = "/data_1/CMIP6/tidy/pcwd_reset/test/",
      fileprefix = "pcwd"
    ))
  ) |>
  collect() |>
  tidyr::unnest(out)

readr::write_rds(
  df_pcwd,
  paste0(
    indir,
    fileprefix,
    "_ANNMAX.rds"
  )
)



# # un-parallel alternative
# df <- tibble(ilon = seq(60)) |>     # vec_index
#   dplyr::mutate(out = purrr::map(
#     ilon,
#     ~collect_cwd_annmax_byilon(
#       .,
#       indir = "~/data/cmip6-ng/tidy/cwd/",
#       fileprefix = "evspsbl_cum"
#     ))
#   ) |>
#   tidyr::unnest(out)
#
# format(object.size(df), units = "MB")

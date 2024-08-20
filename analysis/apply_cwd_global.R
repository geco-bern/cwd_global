#!/usr/bin/env Rscript

# script is called without any arguments
# (By specifying overwrite you can choose within the script to avoid calculating
# or recalculate previous results)

# Example:
# >./apply_cwd_global.R

library(dplyr)
library(map2tidy)
library(multidplyr)

source(paste0(here::here(), "/R/cwd_byilon.R"))
source(paste0(here::here(), "/R/my_cwd.R")) # load function that will be applied to time series

indir  <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/evspsbl/"
outdir <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd/"

dir.create(outdir, showWarnings = FALSE)

# 1) Define filenames of files to process:  -------------------------------
filnams <- list.files(
  indir,
  pattern = "evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_LON_[0-9.+-]*rds",
  full.names = TRUE
)

if (length(filnams) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}


# 2) Setup parallelization ------------------------------------------------
# parallelize job across cores on a single node
ncores <- 6 # parallel::detectCores() # number of cores of parallel threads

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
    my_cwd    = my_cwd,    # make the function known for each core
    cwd_byLON = cwd_byLON, # make the function known for each core
    outdir    = outdir
  )


# 3) Process files --------------------------------------------------------
out <- tibble(in_fname = filnams) |>
  multidplyr::partition(cl) |>      # remove this line to deactivate parallelization
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~cwd_byLON(
      filnam = .,
      outdir = outdir,
      overwrite = FALSE
    ))
  ) |>
  collect() # collect partitioned data.frame

out |> unnest(out)

# TO CHECK: readRDS("/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd//evspsbl_cum_LON_+0.000.rds") |> unnest(data)

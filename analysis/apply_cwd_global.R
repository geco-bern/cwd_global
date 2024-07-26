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
indir  <- "/data_2/scratch/fbernhard/cmip6-ng/tidy/evspsbl/"
outdir <- "/data_2/scratch/fbernhard/cmip6-ng/tidy/cwd/"
dir.create(outdir, showWarnings = FALSE)

filnams <- list.files(
  indir,
  pattern = "evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_.*rds",
  full.names = TRUE
)
list_LON <- gsub(".*(LON_[//-//.//+0-9]*).rds", "\\1", filnams)
# as.numeric(gsub("LON_","",list_LON))

print("getting data for longitude indices:")
# number of cores of parallel threads
ncores <- 6 # parallel::detectCores()

# parallelize job
# load function that will be applied to time series
source(paste0(here::here(), "/R/my_cwd.R"))

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
    my_cwd = my_cwd,   # make the function known for each core
    cwd_byLON = cwd_byLON,   # make the function known for each core
    indir = indir,
    outdir = outdir
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
out <- tibble(LON_string = list_LON) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    LON_string,
    ~cwd_byLON(
      .,
      indir = indir,
      outdir = outdir,
      fileprefix = "evspsbl_cum",
      overwrite = FALSE
      ))
    ) |>
  collect() # collect partitioned data.frame

out |> unnest(out)
# out |> unnest(out) |> unnest(data)

# TO CHECK: readRDS("/data_2/scratch/fbernhard/cmip6-ng/tidy/cwd//evspsbl_cum_LON_+0.000.rds") |> unnest(data)

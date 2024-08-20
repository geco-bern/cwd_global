#!/usr/bin/env Rscript

# script is called without any arguments
# (By specifying overwrite you can choose within the script to avoid calculating
# or recalculate previous results)

# Example:
# >./apply_cwd_global.R

library(dplyr)
library(map2tidy)
library(multidplyr)

source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))

indir  <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/evspsbl/"
outdir <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd/"
dir.create(outdir, showWarnings = FALSE)

# 1) Define filenames of files to process:  -------------------------------
infile_pattern  <- "evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_LON_[0-9.+-]*rds"
outfile_pattern <- "CWD_result_[LONSTRING].rds" # must contain [LONSTRING]

filnams <- list.files(indir, pattern = infile_pattern, full.names = TRUE)
if (length(filnams) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}

# 1b) Define function to apply to each location:  -------------------------------
# function to apply to each file:
source(paste0(here::here(), "/R/my_cwd.R")) # load function that will be applied to time series
# test and debug:
#     df_of_one_coordinate <- read_rds(filnams[1])$data[[1]]
#     my_cwd(df_of_one_coordinate)


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
    apply_fct_to_each_file = apply_fct_to_each_file, # make the function known for each core
    my_cwd                 = my_cwd,                 # make the function known for each core
    outdir                 = outdir,
    outfile_pattern        = outfile_pattern
  )


# 3) Process files --------------------------------------------------------
out <- tibble(in_fname = filnams) |>
  multidplyr::partition(cl) |>      # remove this line to deactivate parallelization
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~apply_fct_to_each_file(
      fct_to_apply_per_location = my_cwd,
      filnam = .,
      outdir = outdir,
      overwrite = FALSE,
      outfilename_template = outfile_pattern # must contain [LONSTRING]
    ))
  ) |>
  collect() # collect partitioned data.frame

out |> unnest(out)
out$out[1]





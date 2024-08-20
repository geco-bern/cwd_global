#!/usr/bin/env Rscript

# script is called without any arguments

# Example:
# >./collect_cwd_annmax.R

library(dplyr)
library(map2tidy)
library(multidplyr)

indir  <- "/data_2/scratch/fbernhard/cmip6-ng/tidy/cwd/"

# 1) Define filenames of files to process:  -------------------------------
filnams <- list.files(
  indir,
  pattern = "evspsbl_cum_LON_[0-9.+-]*_ANNMAX.rds", # make sure to include only _ANNMAX.rds
  full.names = TRUE
)

    # NOTE: this script has to be run non-parallel, since it collects results
    #       from previous scripts that were run in parallel.
    # # 2) Setup parallelization ------------------------------------------------
    # # 2a) Split job onto multiple nodes
    # #     i.e. only consider a subset of the files (others might be treated by another compute node)
    # vec_index <- map2tidy::get_index_by_chunk(
    #   as.integer(args[1]),  # counter for compute node
    #   as.integer(args[2]),  # total number of compute node
    #   length(filnams)       # total number of longitude indices
    # )
    #
    # # 2b) Parallelize job across cores on a single node
    # ncores <- 2 # parallel::detectCores() # number of cores of parallel threads
    #
    # cl <- multidplyr::new_cluster(ncores) |>
    #   # set up the cluster by sending required objects to each core
    #   multidplyr::cluster_library(c("map2tidy",
    #                                 "dplyr",
    #                                 "purrr",
    #                                 "tidyr",
    #                                 "readr",
    #                                 "here",
    #                                 "magrittr")) |>
    #   multidplyr::cluster_assign(
    #     collect_cwd_annmax_byilon = collect_cwd_annmax_byilon  # make the function known for each core
    #   )


# 3) Process files --------------------------------------------------------
global_df <- lapply(filnams,
              function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

readr::write_rds(global_df,
                 file.path(indir,paste0(fileprefix, "_ANNMAX.rds"))





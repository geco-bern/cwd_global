#!/usr/bin/env Rscript

# script is called without any arguments

# Example:
# >./collect_cwd_annmax.R

library(dplyr)
library(map2tidy)
library(multidplyr)

indir   <- "/data_2/scratch/fbernhard/cmip6-ng/tidy/cwd/"
outfile <- "/data_2/scratch/fbernhard/cmip6-ng/tidy/evspsbl_cum_global.nc"

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

# readr::write_rds(global_df,
#                  file.path(indir,paste0(fileprefix, "_ANNMAX.rds"))
# Instead of writing rds file, directly save as NetCDF:


# 4) Output to single, global NetCDF file ---------------------------------
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

# create object that can be used with write_nc2()
global_df <- global_df |>
  select(lon, lat, year, evspsbl_cum) |>
  arrange(year, lat, lon)

arr <- array(
  unlist(global_df$evspsbl_cum),
  dim = c(
    length(unique(global_df$lon)),
    length(unique(global_df$lat)),
    length(unique(global_df$year))
  )
)

# image(arr[,,1])

# create object for use in rgeco::write_nc2()
obj <- list(
  lon = sort(unique(global_df$lon)),
  lat = sort(unique(global_df$lat)),
  time = lubridate::ymd(
    paste0(
      sort(unique(global_df$year)),
      "-01-01"   # taking first of January as a mid-point for each year
    )
  ),
  vars = list(evspsbl_cum = arr)
)


# Get meta information on code executed:
# gitrepo_hash = system("git rev-parse HEAD", intern=TRUE)
gitrepo_hash = system("git rev-parse --short HEAD", intern=TRUE)
gitrepo_status <-
  ifelse(system("git status --porcelain | wc -l", intern = TRUE) == "0",
         "",  #-clean-repository
         "-dirty-repository")
gitrepo_id <- paste0(
  "https://github.com/geco-bern/cwd_global@",
  gitrepo_hash, gitrepo_status)

# Write NetCDF file:
rgeco::write_nc2(
  obj,
  varnams = "evspsbl_cum",
  make_tdim = TRUE,
  path = outfile,
  units_time = "days since 2001-01-01",
  att_title      = "Global Cumulative Water Deficit",
  att_history    = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), gitrepo_id, indir)
)





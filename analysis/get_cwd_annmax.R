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

source(paste0(here::here(), "/R/apply_fct_to_each_file.R"))

indir  <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd"
outdir <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/cwd_annmax"
dir.create(outdir, showWarnings = FALSE)

# 1a) Define filenames of files to process:  -------------------------------
infile_pattern  <- "CWD_result_LON_[0-9.+-]*.rds"
outfile_pattern <- "CWD_result_[LONSTRING]_ANNMAX.rds" # must contain [LONSTRING]

filnams <- list.files(indir, pattern = infile_pattern, full.names = TRUE)
if (length(filnams) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}

# 1b) Define function to apply to each location:  -------------------------------
# function to apply to get annual maximum:
get_annmax <- function(df_of_one_coordinate){
  df_of_one_coordinate |>
    mutate(year = lubridate::year(datetime)) |>
    group_by(year) |>
    summarise(evspsbl_cum = max(evspsbl_cum))
}
# test and debug:
#     df_of_one_coordinate <- read_rds(filnams[1])$data[[1]]
#     df_of_one_coordinate


# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by another compute node)
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for compute node
  as.integer(args[2]),  # total number of compute node
  length(filnams)       # total number of longitude indices
)

# 2b) Parallelize job across cores on a single node
ncores <- 2 # parallel::detectCores() # number of cores of parallel threads

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
    apply_fct_to_each_file = apply_fct_to_each_file, # make the function known for each core
    get_annmax             = get_annmax,             # make the function known for each core
    outdir                 = outdir,
    outfile_pattern        = outfile_pattern
  )


# 3) Process files --------------------------------------------------------
out <- tibble(in_fname = filnams[vec_index]) |>
  multidplyr::partition(cl) |>      # remove this line to deactivate parallelization
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~apply_fct_to_each_file(
      fct_to_apply_per_location = get_annmax,
      filnam = .,
      outdir = outdir,
      overwrite = FALSE,
      outfilename_template = outfile_pattern # must contain [LONSTRING]
    ))
  ) |>
  collect() # collect partitioned data.frame

out |> unnest(out)
out$out[1]





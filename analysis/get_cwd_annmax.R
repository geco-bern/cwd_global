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


# adjust the paths of the indirectory and outdirectory to
# where your cwd and pcwd data is
indir_cwd  <- "/data_1/CMIP6/tidy/cwd_reset/test/" # TODO: indir_cwd       = "/data_2/scratch/fbernhard/CMIP6/tidy/cwd_reset/test/",
indir_pcwd <- "/data_1/CMIP6/tidy/pcwd_reset/test/" # TODO: indir_pcwd      = "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/",
outdir_cwd  <- "/data_2/scratch/fbernhard/CMIP6/tidy/cwd_reset/test/"
outdir_pcwd <- "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/"
dir.create(outdir_cwd, showWarnings = FALSE, recursive = TRUE)
dir.create(outdir_pcwd, showWarnings = FALSE, recursive = TRUE)

# 1a) Define filenames of files to process:  -------------------------------
print("getting data for longitude indices:")
filnams_cwd  <- list.files(indir_cwd,  pattern = "cwd_[0-9]*.rds",  full.names = TRUE)
filnams_pcwd <- list.files(indir_pcwd, pattern = "pcwd_[0-9]*.rds", full.names = TRUE)

if (length(filnams_cwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}
if (length(filnams_pcwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}

# 1b) Define function to apply to each location:  -------------------------------
source(paste0(here::here(), "/R/cwd_annmax_byilon.R"))


# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by another compute node)
#vec_index <- 1:288
vec_index <- sort(as.numeric(gsub("pcwd_([0-9]*).rds","\\1",basename(filnams_pcwd))))

# 2b) Parallelize job across cores on a single node
ncores <- 40 # parallel::detectCores() # number of cores of parallel threads

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
    indir_cwd       = indir_cwd,
    indir_pcwd      = indir_pcwd,
    outdir_cwd      = outdir_cwd,
    outdir_pcwd     = outdir_pcwd,
    cwd_annmax_byilon = cwd_annmax_byilon,   # make the function known for each core
    pcwd_annmax_byilon= pcwd_annmax_byilon
    )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
# 3) Process files --------------------------------------------------------

# Once for cwd
out_cwd <- tibble(ilon = vec_index[1:10]) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~cwd_annmax_byilon(
      .,
      indir       = indir_cwd,
      outdir      = outdir_cwd,
      fileprefix  = "cwd"
      ))
    )

# Once for pcwd
out_pcwd <- tibble(ilon = vec_index) |>
  multidplyr::partition(cl) |>
  dplyr::mutate(out = purrr::map(
    ilon,
    ~cwd_annmax_byilon(
      .,
      indir       = indir_pcwd,
      outdir      = outdir_pcwd,
      fileprefix  = "pcwd"
    ))
  )


out |> collect() |> unnest(out)
out$out[1]





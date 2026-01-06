# to receive arguments to script from the shell
# remotes::install_github("geco-bern/map2tidy@v2.1.4")
# remotes::install_github("geco-bern/rgeco@c2beff5702b4ba7aaef2bcdc46ee96375a3183ac")
# remotes::install_github("geco-bern/rpmodel@v1.2.3")
# # remotes::install_github("geco-bern/cwd@caf1117fbaf72646c15cb8e54a1dc1a917e2683e")
remotes::install_local(path = "../../fabern/cwd/", force = TRUE, upgrade = "never") # to install cwd instead

library(dplyr)
library(map2tidy)
library(multidplyr)
library(terra)
library(tidyr)
library(cwd)
library(rgeco)
library(rpmodel)
library(parallelly)

args <- c(1,1)
# source("~/GitHub/geco-bern/cwd_global/R/ERA5Land/ERA5_simulate_snow.R")
# source("~/GitHub/geco-bern/cwd_global/R/ERA5Land/get_cwd_withSnow_and_reset_ERA5Land.R")
# source("~/GitHub/geco-bern/cwd_global/R/ERA5Land/ERA5Land_compute_pcwd_byLON.R")

source("/storage/homefs/ye23g660/Era5_pcwd/cwd_global/R/ERA5Land-fullRes/ERA5Land_compute_pcwd_byLON.R")
source("/storage/homefs/ye23g660/Era5_pcwd/cwd_global/R/ERA5Land-fullRes/get_cwd_withSnow_and_reset_ERA5Land.R")
source("/storage/homefs/ye23g660/Era5_pcwd/cwd_global/R/ERA5Land-fullRes/ERA5_simulate_snow.R")

indir  <- "/storage/scratch/giub_geco/yelmejjaouy/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/tidy"
outdir <- "/storage/scratch/giub_geco/yelmejjaouy/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/03_pcwd"
dir.create(outdir, showWarnings = FALSE)

# 1a) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"

filnams <- list.files(file.path(indir, "total_prec"),  # use precip folder as example; change year
                      pattern = infile_pattern, full.names = TRUE)
if (length(filnams) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams), ".")
}

# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by
#     another compute node)
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for compute node
  as.integer(args[2]),  # total number of compute node
  length(filnams)       # total number of longitude indices
)

# 2b) Parallelize job across cores on a single node
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads
ncores <- 60

ncores <- 1  # start small
cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster, sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "here",
                                "cwd",
                                "rpmodel",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                              = indir,
    outdir                             = outdir,
    ERA5Land_compute_pcwd_byLON = ERA5Land_compute_pcwd_byLON,   # make the function known for each core
    get_cwd_withSnow_and_reset_ERA5Land = get_cwd_withSnow_and_reset_ERA5Land,
    ERA5_simulate_snow = ERA5_simulate_snow
  )
# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk

# 3) Process files --------------------------------------------------------

out <- tibble(in_fname = filnams[vec_index]) |>
  mutate(LON_string = gsub("^.*?(LON_[0-9.+-]*).rds$", "\\1", basename(in_fname))) |>

  # Define the corresponding output filename based on how your output files are named
  # Assuming output files follow the same LON_string format with .rds extension
  mutate(out_fname = file.path(outdir, paste0("ERA5Land_pcwd_",LON_string, ".rds"))) |>

  # Filter out files that already have corresponding output files
  filter(!file.exists(out_fname)) |>
  # Remove unnecessary columns if needed
  dplyr::select(-in_fname, -out_fname) |>

  multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    LON_string,
    ~ERA5Land_compute_pcwd_byLON(
      .,
      indir           = indir,
      outdir          = outdir))
  ) |> collect()



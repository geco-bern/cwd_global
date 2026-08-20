#!/usr/bin/env Rscript

# script is called with two arguments for parallelization:
# 1. counter for chunks (e.g. # of each compute node)
# 2. total number of chunks (e.g. number of total compute nodes)

# Note that these arguments can be used to distribute over multiple nodes.
# Distribution over CPU cores of a single node is handled by multidplyr
# and argument ncores inside of this script.

# Example for 4 CPU-nodes:
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 2 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 3 4
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 4 4

# Example for 1 CPU-nodes:
# > Rscript 02_apply_pcwd_global_ERA5Land_ubelix.R 1 1
# # When using this script directly from RStudio, not from the shell, specify
# args <- c(1, 180)
#args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)
print(sprintf("%s: Starting 02_apply_pcwd_global_ERA5Land_ubelix.R with arguments: %s",
              Sys.time(),
              paste0(args, collapse = " ")))

# When reloading re-install packages from renv.lock by doing:
library(renv)
source("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land-lowRes/renv/activate.R") # somewhat on UBELIX this needs to be done manually sometimes
renv::project()
options(renv.config.pak.enabled = FALSE)
renv::restore(prompt = FALSE)


library(lubridate)
library(dplyr)
library(map2tidy)
library(multidplyr)
library(tidyr)
library(cwd)       # using cwd: v2.0 or newer       # 1. Load cwd package first
library(parallelly)

# Working directory (see main.sh): /storage/homefs/fb24k097/GitHub/geco-bern/cwd_global/analysis/ERA5Land-lowRes
source("../../R/ERA5Land-lowRes/ERA5Land_compute_pcwd_byLON.R")
source("../../R/ERA5Land-lowRes/get_cwd_withSnow_and_reset_ERA5Land.R")
source("../../R/ERA5Land-lowRes/ERA5_simulate_snow.R")



#indir  <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy"
#indir  <- "/storage/capacity/occr_geco/data_2/archive_projects/MSc_2025_phelpap_cwdModESim/ERA5Land_1950-2024/ERA5Land_regridded/" # not stored in archive_projects
indir  <- "/storage/scratch/giub_geco/fbernhard/ERA5Land-lowRes_2025-2026/tidy"
outdir <- "/storage/scratch/giub_geco/fbernhard/ERA5Land-lowRes_2025-2026/02_daily_pcwd"
dir.create(outdir, showWarnings = FALSE)


# 1) Define filenames of files to process:  -------------------------------
infile_pattern  <- "*.rds"

filnams <- list.files(file.path(indir, "tot_tp"),  # use tot_tp folder to get all LON
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
# ncores <- 1 # start small
# ncores <- 180
ncores <- length(parallelly::availableWorkers()) # parallel::detectCores() # number of cores of parallel threads

cl <- multidplyr::new_cluster(ncores) |>
  # set up the cluster, sending required objects to each core
  multidplyr::cluster_library(c("map2tidy",
                                "dplyr",
                                "purrr",
                                "tidyr",
                                "readr",
                                "lubridate",
                                "cwd",
                                "magrittr")) |>
  multidplyr::cluster_assign(
    indir                               = indir,
    outdir                              = outdir,
    ERA5Land_simulate_snow              = ERA5Land_simulate_snow,             # make the function known for each core
    ERA5Land_compute_pcwd_byLON         = ERA5Land_compute_pcwd_byLON,        # make the function known for each core
    get_cwd_withSnow_and_reset_ERA5Land = get_cwd_withSnow_and_reset_ERA5Land # make the function known for each core
  )
# FOR DEVELOPMENT:
# LON_string <- gsub(".rds","", gsub("^.*(LON_)", "\\1", filnams[1]))
# # debug(get_cwd_withSnow_and_reset_ERA5Land)
# # debug(ERA5Land_compute_pcwd_byLON)
# ERA5Land_compute_pcwd_byLON(LON_string, indir, outdir)

# 3) Process files --------------------------------------------------------
#    If some are missing from the 3600 simply rerun this again manually. It should filter out already processed files.
out <- tibble(in_fname = filnams[vec_index]) |>
  mutate(LON_string = gsub("^.*?(LON_[0-9.+-]*).rds$", "\\1", basename(in_fname))) |>

  # Define the corresponding output filename based on how your output files are named
  # Assuming output files follow the same LON_string format with .rds extension
  mutate(out_fname = file.path(outdir, paste0("ERA5Land_pcwd_",LON_string, ".rds"))) |>
  mutate(out_fname = file.path(outdir, paste0("ERA5Land_pcwd_",LON_string, ".rdsallyears_onlypcwd_mm.rds"))) |> # actually to correspond to variant 1 from ERA5Land_compute_pcwd_byLON.R


  # Filter out files that already have corresponding output files
  filter(!file.exists(out_fname)) %>% {
    print(sprintf("%s: Starting pcwd computation for %d LONs:",Sys.time(),nrow(.)));
          cat(paste0(.$LON_string,collapse="\n")); cat("\n")
          .} |>
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

print(out)

print(out |> unnest(out) |> filter(grepl("Failed", out)))

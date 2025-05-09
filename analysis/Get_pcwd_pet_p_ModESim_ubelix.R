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
#args <- c(1, 1)

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==2)

library(dplyr)
library(map2tidy)
library(multidplyr)
library(ncdf4)


# adjust the paths of the indirectory and outdirectory to
# where your cwd and pcwd data is
indir   <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m020_tidy/02_pcwd_1420"
outdir  <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/test_daily"
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# 1a) Define filenames of files to process:  -------------------------------
filnams_pcwd <- list.files(indir, pattern = "ModESim_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)
# # 1a) Define filenames of files to process:  -------------------------------
# filnams_pcwd <- list.files(indir, pattern = "ERA5Land_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)

# if (length(filnams_pcwd) <= 1){
#   stop("Should find multiple files. Only found " ,length(filnams), ".")
# }

# 1b) Define function to apply to each location:  -------------------------------
source("/storage/homefs/ph23v078/cwd_global/R/extract_p_pet.R")

# 1c) Define volcanic eruption years to be extracted:

#####read in volcanic data used to select years:
#AOD years 1000 C.E. to 1900 C.E.:
input_file <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/ModESim_forcings/1420_1/eva_holo2.2_forcing_echam_T63_ir_1000-1900.nc"
nc_forcings <- nc_open(input_file)
aod = ncvar_get(nc_forcings, varid="aod")[6,,] # index 6 for 500nm wavelength
lat = ncvar_get(nc_forcings, varid="lat")
time_1420 = ncvar_get(nc_forcings, varid="time")
time_units <- ncatt_get(nc_forcings, "time", "units")  # Check the time units (e.g., "days since 0001-01-01")
nc_close(nc_forcings)

########### Calculate annual mean values for first epoch years:
# Define the years of interest
years_of_interest <- 1420:1849

# Find indices of columns corresponding to the desired years
selected_indices <- which(time_1420 %in% years_of_interest)

# Subset the matrix to include only those years
aod_selected <- aod[, selected_indices]
time_selected <- time_1420[selected_indices]  # Corresponding time values

# Create the corresponding months (repeating each year 12 times)
months <- rep(1:12, length(time_selected) / 12)  # Assuming 12 months for each year

# Create the "yyyy-mm" format using the year and month
dates <- paste(time_selected, sprintf("%02d", months), sep = "-")  # sprintf adds leading zero for months < 10

# Now dates is in "yyyy-mm" format
head(dates)  # Check the first few entries

# Compute the mean over latitudes (first dimension)
mean_latitude <- apply(aod_selected, 2, mean, na.rm = TRUE)

# Convert to a data frame for easier manipulation
aod_1420_df <- data.frame(dates = dates, mean_value = mean_latitude)
aod_1420_df$dates <- lubridate::ym(aod_1420_df$dates)
selected_aod <- aod_1420_df %>% dplyr::filter(mean_value >= 0.01)
selected_aod$year <- format(selected_aod$dates, "%Y")

# Get unique years with volcanic eruptions
eruption_years <- as.numeric(unique(selected_aod$year))
# Generate a sequence of years for each eruption year (-2 to +2)
selected_years <- unique(unlist(lapply(eruption_years, function(year) seq(year - 2, year + 2))))



# 2) Setup parallelization ------------------------------------------------
# 2a) Split job onto multiple nodes
#     i.e. only consider a subset of the files (others might be treated by another compute node)
vec_index <- map2tidy::get_index_by_chunk(
  as.integer(args[1]),  # counter for compute node
  as.integer(args[2]),  # total number of compute node
  length(filnams_pcwd)   # total number of longitude indices
)

# 2b) Parallelize job across cores on a single node
ncores <- 50 # parallel::detectCores() # number of cores of parallel threads

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
    indir       = indir,
    outdir      = outdir,
    extract_selected_data = extract_selected_data,
    process_cwd_extract_data = process_cwd_extract_data,   # make the function known for each core
    selected_years = selected_years
  )

# distribute computation across the cores, calculating for all longitudinal
# indices of this chunk
# 3) Process files --------------------------------------------------------

# Once for pcwd

#in_fname <- in_fname[1]

out_pcwd <- tibble(in_fname = filnams_pcwd[vec_index]) |>
  multidplyr::partition(cl) |>    # comment this partitioning for development
  dplyr::mutate(out = purrr::map(
    in_fname,
    ~process_cwd_extract_data(
      .,
      outdir          = outdir,
      selected_years = selected_years))
  ) |> collect()


#### test: read in finished rds file
#check <- readRDS("/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/test_daily/ModESim_pcwd_LON_-001.875_EXTRACTED.rds")

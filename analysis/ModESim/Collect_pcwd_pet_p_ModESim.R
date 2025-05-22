#!/usr/bin/env Rscript

library(dplyr)
library(tidyr)
library(readr)
library(lubridate)
library(rgeco)

# Paths:
indir <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/test_p_pet/m001"
outdir_nc <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/test_p_pet/collected_p_pet/m001"
dir.create(outdir_nc, showWarnings = FALSE, recursive = TRUE)

# List of variables you want to process
varnames <- c("tot_precip", "tot_pet", "max_deficit")# We'll create a list of open ncdf4 objects, one per variable.
filnams <- list.files(indir, pattern = paste0("ModESim_pcwd_(LON_[0-9.+-]*)_tot_p_pet.rds"), full.names = TRUE)

# Function to read and process files for each variable
process_variable_data <- function(varname) {
  # Define filenames based on the variable
  #filnams <- filnams[1]

  # 1) Read and combine all files for this variable
  df <- lapply(filnams, function(filnam) {
    readr::read_rds(filnam) |> tidyr::unnest(data)
  }) |> bind_rows()

  # 2) Prepare data for writing to NetCDF
  prepare_write_nc <- function(df, varname) {
    df <- df |> dplyr::select(lon, lat, year, all_of(varname)) |> arrange(year, lat, lon)

    arr <- array(
      unlist(df[[varname]]),
      dim = c(
        length(unique(df$lon)),
        length(unique(df$lat)),
        length(unique(df$year))
      )
    )

    vars_list = list(arr)
    names(vars_list) <- varname

    #convert numeric years to Date (Jan 1 of each year)
    years   <- sort(unique(df$year))
    dates   <- as.Date(paste0(years, "-01-01"))

    obj <- list(
      lon = sort(unique(df$lon)),
      lat = sort(unique(df$lat)),
      time = dates,
      vars = vars_list
    )

    return(obj)
  }

  obj <- prepare_write_nc(df, varname)

  # 3) Write NetCDF file
  outfile_nc <- file.path(outdir_nc, paste0(varname, "_tot_p_pet.nc"))

  # Get meta information on code executed:
  get_repo_info <- function(){
    gitrepo_url  <- system("git remote get-url origin", intern=TRUE)
    gitrepo_hash <- system("git rev-parse --short HEAD", intern=TRUE)
    gitrepo_status <-
      ifelse(system("git status --porcelain | wc -l", intern = TRUE) == "0",
             "",  #-clean-repository
             "-dirty-repository")
    gitrepo_id <- paste0(
      gsub(".git$", "", gsub(".*github.com:","github.com/", gitrepo_url)),
      "@", gitrepo_hash, gitrepo_status)

    return(gitrepo_id)
  }
  get_repo_info()

  rgeco::write_nc2(
    obj,
    varnams = varname,
    make_tdim = TRUE,
    path = outfile_nc,
    units_time = "days since 2001-01-01",
    att_title = paste("Annual totals and max PCWD for volcanic yeras for ", varname),
    att_history = sprintf(
      "Created on: %s, with R scripts from (%s) processing input data from: %s",
      Sys.Date(), get_repo_info(), indir
    )
  )

  message("NetCDF for ", varname, " written to ", outfile_nc)
}

# Iterate over variables and process them
for (varname in varnames) {
  process_variable_data(varname)
}

message("All NetCDF files have been created successfully.")


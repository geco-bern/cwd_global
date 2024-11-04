#!/usr/bin/env Rscript

# script is called without any arguments

# Example:
# >./collect_cwd_annmax.R

library(dplyr)
library(map2tidy)
library(multidplyr)
library(tidyr)
library(lubridate)
library(purrr)

indir        <- "~/scratch2/m001_tidy"
outfile_pcwd <- "~/scratch2/m001_tidy/02_5_pcwd_result/PCWD_deficit" # adjust path to where the file should be written to

#indir        <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_pcwd_1850"
#outfile_pcwd <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_5_pcwdresult_1850" # adjust path to where the file should be written to


# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd <- list.files(indir, pattern = "ModESim_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)

# if (length(filnams_cwd) <= 1){
#   stop("Should find multiple files. Only found " ,length(filnams_cwd), ".")
# }

# 3) Process files --------------------------------------------------------
df_pcwd_2 <- lapply(filnams_pcwd,
                  function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

dir.create(dirname(outfile_pcwd), showWarnings = FALSE, recursive = TRUE)
readr::write_rds(
  df_pcwd,
  paste0(outfile_pcwd, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, ".rds"))



# 4) Output to global NetCDF file ---------------------------------
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

#prepare_write_nc2_no_aggregation <- function(df_pcwd_2, varname) {

  # Step 1: Extract unique lat and lon coordinates
  lons <- sort(unique(df_pcwd_2$lon))
  lats <- sort(unique(df_pcwd_2$lat))

  # Step 1.5: Extract unique dates across all `record` tibbles in `data`
  unique_dates <- df_pcwd_2$data %>%
    map(~ .x$date) %>%
    unlist() %>%
    unique() %>%
    sort()

  # Initialize a list to store each variable's array data
  vars_list <- list()

  # Step 2: Process each record in `df_pcwd_2`
  for (i in 1:nrow(df_pcwd_2)) {
    record <- df_pcwd_2$data[[i]]
    coord_lon <- df_pcwd_2$lon[i]
    coord_lat <- df_pcwd_2$lat[i]

    # Step 3: Populate arrays for each variable in `record` without aggregation
    for (var_name in names(record)[-1]) {  # Exclude 'date' column
      if (!exists(var_name, envir = vars_list)) {
        # Initialize the array to hold data for this variable
        vars_list[[var_name]] <- array(NA,
                                       dim = c(length(lons), length(lats), length(unique_dates)))
      }

      # Find the indices for lon, lat, and time dimensions
      lon_index <- which(lons == coord_lon)
      lat_index <- which(lats == coord_lat)
      date_indices <- match(record$date, unique_dates)

      # Fill in data for this variable
      vars_list[[var_name]][lon_index, lat_index, date_indices] <- record[[var_name]]
    }
  }

  # Step 4: Prepare the final object for writing
  obj <- list(
    lon = lons,
    lat = lats,
    time = unique_dates,  # Exact dates are retained as time dimension
    vars = vars_list
  )

  return(obj)
}

# Usage example
obj_pcwd <- prepare_write_nc2_no_aggregation(df_pcwd_2, varname="pcwd")



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

# Write NetCDF file:

rgeco::write_nc2(
  obj_pcwd,
  varnams = "pcwd",
  make_tdim = TRUE,
  path = paste0(outfile_pcwd, ".nc"),
  units_time = "days since 2001-01-01",
  att_title      = "Global Potential Cumulative Water Deficit",
  att_history    = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), get_repo_info(), indir)
)






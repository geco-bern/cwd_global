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

#indir        <- "~/scratch2/m001_tidy"
outfile_pcwd_def <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_2_pcwd_result/PCWD_deficit" # adjust path to where the file should be written to
outfile_pcwd_inst <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_2_pcwd_result/PCWD_instance"
indir_inst        <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_1_pcwd_inst_1850"
indir_def        <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_1_pcwd_def_1850"
#outfile_pcwd <- "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m001_tidy/02_2_pcwdresult_1850" # adjust path to where the file should be written to


# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd_def <- list.files(indir_def, pattern = "ModESim_pcwd_(LON_[0-9.+-]*)_DEFICIT.rds", full.names = TRUE)
filnams_pcwd_inst <- list.files(indir_inst, pattern = "ModESim_pcwd_(LON_[0-9.+-]*)_INST.rds", full.names = TRUE)

# if (length(filnams_cwd) <= 1){
#   stop("Should find multiple files. Only found " ,length(filnams_cwd), ".")
# }

# 3) Process files --------------------------------------------------------
df_pcwd_def <- lapply(filnams_pcwd_def,
                    function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

dir.create(dirname(outfile_pcwd_def), showWarnings = FALSE, recursive = TRUE)
readr::write_rds(
  df_pcwd_def,
  paste0(outfile_pcwd_def, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, ".rds"))

df_pcwd_inst <- lapply(filnams_pcwd_inst,
                    function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

dir.create(dirname(outfile_pcwd_inst), showWarnings = FALSE, recursive = TRUE)
readr::write_rds(
  df_pcwd_inst,
  paste0(outfile_pcwd_inst, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, ".rds"))



# 4) Output to global NetCDF file ---------------------------------
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

#####write deficit netcdf

prepare_write_nc2_def <- function(df_cwd, varname_list) {
  # Ensure the provided variable names match columns in df_cwd
  df_cwd <- df_cwd |>
    dplyr::select(lon, lat, date, all_of(varname_list)) |>
    arrange(date, lat, lon)

  # Initialize an empty list to store arrays for each variable
  vars_list <- list()

  # Loop through each variable in varname_list to create an array and add to vars_list
  for (varname in varname_list) {
    arr <- array(
      unlist(df_cwd[[varname]]),
      dim = c(
        length(unique(df_cwd$lon)),
        length(unique(df_cwd$lat)),
        length(unique(df_cwd$date))
      )
    )

    vars_list[[varname]] <- arr  # Add each array to the list with its variable name

  }
  # Define the object structure for netCDF writing
  obj <- list(
    lon = sort(unique(df_cwd$lon)),
    lat = sort(unique(df_cwd$lat)),
    time = sort(unique(df_cwd$date)),  # Use daily dates as the time dimension
    vars = vars_list
  )

  return(obj)
}

varnames <- c("deficit", "precip", "pet")
obj_pcwd_def <- prepare_write_nc2_def(df_pcwd_def, varname_list = varnames)

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

# Run the adjusted `write_nc2()` command
# Define variable names and units
varnames <- c("deficit", "precip", "pet")  # Names of your variables
units <- c("mm", "mm/day", "mm/day")  # Corresponding units for each variable
long_names <- c("Cumulative Water Deficit", "Precipitation", "Potential Evapotranspiration")  # Long names for each variable

# Call write_nc2 with obj_pcwd_def
rgeco::write_nc2(
  obj = obj_pcwd_def,                   # Pass the prepared object
  varnams = varnames,                   # Specify the variable names
  make_tdim = TRUE,                     # Create a time dimension
  path = paste0(outfile_pcwd_def, ".nc"), # Output file path
  units_time = "days since 2001-01-01", # Time units
  units = units,                        # Units for each variable
  long_names = long_names,              # Long names for each variable
  att_title = "Global Potential Cumulative Water Deficit and Additional Variables", # Title attribute
  att_history = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), get_repo_info(), indir_def
  )
)


#####write instance netcdf

# Adjust the function to handle all required variables, including dates
prepare_write_nc2_inst <- function(df_cwd, varname_list) {
  # Ensure the provided variable names match columns in df_cwd
  df_cwd <- df_cwd |>
    dplyr::select(lon, lat, all_of(varname_list)) |>
    arrange(lat, lon)

  # Initialize an empty list to store arrays for each variable
  vars_list <- list()

  # Loop through each variable in varname_list to create an array and add to vars_list
  for (varname in varname_list) {
    arr <- array(
      unlist(df_cwd[[varname]]),
      dim = c(
        length(unique(df_cwd$lon)),
        length(unique(df_cwd$lat))
      )
    )

    vars_list[[varname]] <- arr  # Add each array to the list with its variable name

  }
  # Define the object structure for netCDF writing
  obj <- list(
    lon = sort(unique(df_cwd$lon)),
    lat = sort(unique(df_cwd$lat)),
   # time = sort(unique(df_cwd$date)),  # Use daily dates as the time dimension
    vars = vars_list
  )

  return(obj)
}


# Assuming df_pcwd_inst is your input dataframe
varnames <- c("deficit", "len", "date_start", "date_end")  # Exclude date_end from varnames since we will handle it separately
obj_pcwd_inst <- prepare_write_nc2_inst(df_pcwd_inst, varname_list = varnames)

# Get meta information on code executed (as previously defined)
git_repo_info <- get_repo_info()

# Define variable names and units
units <- c("mm", "days", "days since 2001-01-01", "days since 2001-01-01")  # Corresponding units for deficit and length
long_names <- c("Cumulative Water Deficit", "Event Length", "Event start date", "Event end date")  # Long names for each variable

# Write the NetCDF file
rgeco::write_nc2(
  obj = obj_pcwd_inst,                             # Pass the prepared object
  varnams = c(varnames),  # Include both date variables in variable names
  make_tdim = FALSE,                                 # Create a time dimension
  path = paste0(outfile_pcwd_inst, ".nc"),         # Output file path
  #units_time = "days since 2001-01-01",             # Time units
  units = c(units),                 # Units for date_start and date_end
  long_names = c(long_names),  # Long names for date_start and date_end
  att_title = "Global Potential Cumulative Water Deficit Instances and Length",  # Title attribute
  att_history = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), git_repo_info, indir_inst
  )
)



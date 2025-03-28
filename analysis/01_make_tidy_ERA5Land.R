#!/usr/bin/env Rscript

# script is called without any arguments

Sys.getpid()

library(map2tidy)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(ncdf4)

# list demo file path
# adjust path to where your ERA5Land data is located
path_ERA5Land <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded" #uses input that has been regridded; original data has dimension names latitude and longitude instead of lat lon
outdir <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy"

##### Adapted workflow for tidy to fill latitudes without values with NA

# Function to extract expected latitudes from a sample NetCDF file
get_latitudes <- function(nc_file) {
  nc <- nc_open(nc_file)
  latitudes <- ncvar_get(nc, "lat")  # Adjust if the latitude variable name differs
  nc_close(nc)
  return(sort(latitudes))
}

# Extract the expected latitude range from a sample file
sample_nc <- list.files(path_ERA5Land, pattern = ".nc", full.names = TRUE)[1]
expected_lat <- get_latitudes(sample_nc)

fill_missing_latitudes <- function(df, varname) {
  existing_lats <- unique(df$lat)
  missing_lats <- setdiff(expected_lat, existing_lats)

  # Function to generate missing data for a given latitude
  generate_missing_data <- function(lat_value, reference_data) {
    tibble(
      lon = unique(reference_data$lon),  # Assume longitude remains the same
      lat = lat_value,
      data = list(tibble(
        !!sym(varname) := NA_real_,  # Dynamically assign variable name , # Fill missing values with NA
        datetime = reference_data$data[[1]]$datetime  # Copy datetime structure

      ))
    )
  }

  # Generate missing rows and append to df
  if (length(missing_lats) > 0) {
    reference_data <- df[1, ]  # Use the first row as a reference for datetime
    missing_rows <- map_df(missing_lats, ~generate_missing_data(.x, reference_data))
    df <- bind_rows(df, missing_rows) %>% arrange(lat)
  }

  return(df)
}

# Function to process and fix missing latitudes
process_variable <- function(varnam, fileprefix, output_subdir) {
  filnam <- list.files(path_ERA5Land, pattern = ".nc", full.names = TRUE)
  output_dir <- file.path(outdir, output_subdir)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  # Run map2tidy as normal (files are written to disk)
  res <- map2tidy(
    nclist = filnam,
    varnam = varnam,
    lonnam = "lon",
    latnam = "lat",
    timenam = "valid_time",
    do_chunks = TRUE,
    outdir = output_dir,  # Writing file with only land latitudes
    fileprefix = fileprefix,
    ncores = 1,
    overwrite = TRUE
  )

  # Read back files, fix missing latitudes, and overwrite them
  rds_files <- list.files(output_dir, pattern = "\\.rds$", full.names = TRUE)
  walk(rds_files, function(rds_file) {
    df <- readRDS(rds_file)
    df_fixed <- fill_missing_latitudes(df, varnam)
    saveRDS(df_fixed, rds_file)  # Overwrite the file to contain all latitudes, filled with NA
  })

  message("Processing complete for: ", varnam)
}

# Process Precipitation (tot_tp)
process_variable("tot_tp", "ERA5Land_UTCDaily_tottp", "total_prec")

# Process Potential Evapotranspiration (tot_pev)
process_variable("tot_pev", "ERA5Land_UTCDaily_totpev", "total_pet")


##### Original tidy workflow without NA filling:

# # Precipitation - daily resolution --------------------------------------------------
# varnam <- "tot_tp"
# filnam <- list.files(
#   paste0(path_ERA5Land),
#   pattern = ".nc", full.names = TRUE)
# output_dir <- file.path(outdir, "total_prec")
#
# # Convert to tidy
# res_pr <- map2tidy(
#   nclist = filnam,
#   varnam = "tot_tp",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "valid_time",
#   do_chunks = TRUE,
#   outdir = output_dir,
#   fileprefix = "ERA5Land_UTCDaily_tottp",
#   ncores = 1,
#   overwrite = FALSE
# )
#
# # For each nested data frame, complete the grid to include all expected latitudes.
# # Missing tot_tp values will be filled with NA.
# res_pr <- res_pr %>%
#   mutate(data = map(data, ~ .x %>%
#                       complete(lon, valid_time, lat = expected_lat,
#                                fill = list(tot_tp = NA))))
#
# # Check if any unsuccessful:
# stopifnot(nrow(res_pr |> unnest(data) |> filter(!grepl("Written", data))) == 0)
#
# # Potential Evapotranspiration - monthly resolution -------------------------------
# varnam <- "tot_pev"
# filnam <- list.files(
#   paste0(path_ERA5Land),
#   pattern = ".nc", full.names = TRUE)
# output_dir <- file.path(outdir, "total_pet")
#
# # Convert to tidy
# res_pev <- map2tidy(
#   nclist = filnam,
#   varnam = "tot_pev",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "valid_time",
#   do_chunks = TRUE,
#   outdir = output_dir,
#   fileprefix = "ERA5Land_UTCDaily_totpev",
#   ncores = 1,
#   overwrite = FALSE
# )
#
# # Complete the grid for tot_pev as well
# res_pev <- res_pev %>%
#   mutate(data = map(data, ~ .x %>%
#                       complete(lon, valid_time, lat = expected_lat,
#                                fill = list(tot_pev = NA))))
#
# # Check if any unsuccessful:
# stopifnot(nrow(res_pev |> unnest(data) |> filter(!grepl("Written", data))) == 0)

# Define expected number of latitudes
expected_nlat <- 96

# Get a list of all RDS files that match the naming pattern
rds_files <- list.files(path = "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy/mean_sp",
                       pattern = "^ERA5Land_UTCDaily_sp_LON_.*\\.rds$", full.names = TRUE)

# rds_files <- list.files(path = "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m010_tidy/02_pcwd_1850",
#                         pattern = "^ModESim_pcwd_LON_.*\\.rds$", full.names = TRUE)

rds_file <- rds_files[152]
test <- readRDS(rds_file)
sum(is.na(test$data))
# Initialize vector to record any files with a mismatch
mismatch_files <- c()

for (file in rds_files) {
  # Load the data object
  obj <- readRDS(file)

  # Try to determine how the latitudes are stored.
  # If the object is a matrix or array, assume the first dimension is latitude.
  dims <- dim(obj)
  if (!is.null(dims)) {
    if (dims[1] != expected_nlat) {
      message(sprintf("File %s has %d latitudes instead of %d.", file, dims[1], expected_nlat))
      mismatch_files <- c(mismatch_files, file)
    }
  } else {
    # If there's no dim attribute, maybe the object is a data frame.
    # Check for a column with latitude values (adjust the column name as needed).
    if ("lat" %in% names(obj)) {
      nlat <- length(unique(obj$lat))
      if (nlat != expected_nlat) {
        message(sprintf("File %s has %d unique latitudes instead of %d.", file, nlat, expected_nlat))
        mismatch_files <- c(mismatch_files, file)
      }
    } else {
      message(sprintf("File %s does not have recognizable dimensions or a 'lat' column.", file))
      mismatch_files <- c(mismatch_files, file)
    }
  }
}

if (length(mismatch_files) == 0) {
  message("All files have the expected 96 latitudes.")
} else {
  message("The following files do not have the expected number of latitudes:")
  print(mismatch_files)
}


# # ###########test latitudes of files
# library(ncdf4)
#
# # Function to extract latitude values from a NetCDF file
# get_latitudes <- function(nc_file) {
#   nc <- nc_open(nc_file)
#   lat_var <- NULL
#
#   # Check possible latitude names
#   for (lat_name in c("lat", "latitude", "y")) {
#     if (lat_name %in% names(nc$dim)) {
#       lat_var <- ncvar_get(nc, lat_name)
#       break
#     }
#   }
#
#   if (is.null(lat_var)) {
#     stop(paste("Latitude dimension not found in file:", nc_file))
#   }
#
#   nc_close(nc)
#   return(lat_var)
# }
#
#
# # Define file paths
# main_file <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/ERA5Land_UTCDaily.tp_pev.1968.nc"  # Change to your main NetCDF file
# other_files <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/ERA5Land_UTCDaily.tp_pev.1967.nc"
#
# # Extract latitude from main file
# main_lat <- get_latitudes(main_file)
# other_lat <- get_latitudes(other_files)
#
# # Compare with other files
# for (file in other_files) {
#   other_lat <- get_latitudes(file)
#
#   if (length(main_lat) != length(other_lat)) {
#     cat("Mismatch in latitude array length for:", file, "\n")
#   } else {
#     # Use identical() to check if values match exactly
#     if (identical(main_lat, other_lat)) {
#       cat("Latitude values match for:", file, "\n")
#     } else {
#       cat("Latitude values differ in:", file, "\n")
#     }
#   }
# }
#

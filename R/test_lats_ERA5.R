# Define expected number of latitudes
expected_nlat <- 96

# Get a list of all RDS files that match the naming pattern
rds_files <- list.files(path = "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/tidy/total_pet",
                       pattern = "^ERA5Land_UTCDaily_totpev_LON_.*\\.rds$", full.names = TRUE)

# rds_files <- list.files(path = "/storage/research/giub_geco/data_2/scratch/phelpap/ModESim/m010_tidy/02_pcwd_1850",
#                         pattern = "^ModESim_pcwd_LON_.*\\.rds$", full.names = TRUE)

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

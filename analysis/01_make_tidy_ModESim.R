Sys.getpid()

library(map2tidy)
library(dplyr)
library(stringr)
library(tidyr)

# # list demo file path
# # adjust path to where your ModE-Sim data is located
 outdir <- "~/scratch2/m014_tidy"

# # Precipitation - daily resolution--------------------------------------------------
# path_ModESim <- "~/scratch2"
# varnam <- "precip";
# set_name <- "m014_1420_1"
# filnam <- list.files(
#   paste0(path_ModESim, "/", varnam, "/", set_name),
#   pattern = ".nc", full.names = TRUE)
# output_dir = file.path(outdir, "1420_01_m014_precip")
# prefix <- "set1420_1_m014_precip"
#
# # convert to tidy   -----commented out for running in the shell
# res_pr <- map2tidy(
#   nclist = filnam[1:431],  #[1:431] for 1420 onwards; [1:431] for 1420
#   varnam = "precip",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "time",
#   do_chunks = TRUE,
#   outdir = output_dir,
#   fileprefix = "set1420_1_m014_precip",
#   ncores = 1,
#   overwrite = FALSE
#   #filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
#   )
#
# # # test <- readRDS("~/scratch2/tidy/1420_01_m001_precip/set1420_1_m001_precip_LON_-120.000.rds")
# # #
# # # test %>% slice(1) %>% unnest(data) %>%
# # #   tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
# # #   mutate(datetime = lubridate::ymd(datetime))
# #
# #
# # #####################for single files:
# # # df <- readRDS(file.path(outdir, "/", "1420_01_m001_precip", "set1420_1_m001_precip_LON_-001.875.rds"))
# # #
# # # # apply it to all nested data.frames using purrr::map
# # # df2 <- df |> dplyr::mutate(data = purrr::map(data, function(x){
# # #   x |>
# # #     tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
# # #     dplyr::mutate(date = lubridate::ymd(date)) |> dplyr::select(-fract_day)
# # # }))
# # #
# # # saveRDS(df2, file.path(outdir, "/", "1420_01_m001_precip", "set1420_1_m001_precip_LON_-001.875.rds"))
# #
# #
# #
# # #######loop through remaining files for date conversion:
# #
# # Define directories
# tmpdir <- "~/scratch2/m014_tidy/1420_01_m014_precip"  # directory where your RDS files are located
# outdir_ref <- tmpdir  # Assuming output directory is same as input directory
#
# # List all RDS files in the directory
# rds_files <- list.files(tmpdir, pattern = "\\.rds$", full.names = TRUE)
#
# # List of already processed files (replace with the actual filenames)
# processed_files <- c(
#   ""
# )
#
# # Exclude already processed files
# remaining_files <- rds_files[!basename(rds_files) %in% basename(processed_files)]
#
#
# # # Define the reference time
# # reference_time <- as.POSIXct("2024-11-13 19:16", tz = "CET")
# #
# # # Function to check if a file was created or modified before the reference time
# # needs_processing <- function(file_path) {
# #   # Check if the file modification time is before the reference time
# #   return(file.info(file_path)$mtime < reference_time)
# # }
# #
# # # Identify files that need processing
# # remaining_files <- rds_files[sapply(rds_files, needs_processing)]
#
#
# # Function to process and save each RDS file
# process_file <- function(file_path) {
#   df <- readRDS(file_path)
#
#   # Apply the datetime fix
#   df2 <- df |>
#     dplyr::mutate(data = purrr::map(data, function(x){
#       x |>
#         tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
#         dplyr::mutate(date = lubridate::ymd(date)) |>
#         dplyr::select(-fract_day)
#     }))
#
#   # Save the processed file
#   saveRDS(df2, file_path)  # Overwrite the file with the fixed datetime format
# }
#
# # Loop through the remaining files and process them
# purrr::walk(remaining_files, process_file)
#
#
# # # # check result for the first element:
# # # df2 |> dplyr::slice(1) |> tidyr::unnest(data)

## Temperature - daily resolution-------------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "tsurf";
set_name <- "m014_1850_1"
filnam <- list.files(
  paste0(path_ModESim, "/", varnam, "/", set_name),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1850_01_m014_tsurf")


prefix <- "set1850_1_m014_tsurf"
# # convert to tidy
# res_ts <- map2tidy(
#   nclist = filnam[1:431],
#   varnam = "tsurf",
#   lonnam = "lon",
#   latnam = "lat",
#   timenam = "time",
#   do_chunks = TRUE,
#   outdir = output_dir,
#   fileprefix = "set1850_1_m014_tsurf",
#   ncores = 1,
#   overwrite = FALSE
#   #filter_lon_between_degrees = c(+104, +106) #longitude of US-Ton: -120.9660
# )

#######loop through remaining files for date conversion:

# Define directories
tmpdir <- "~/scratch2/m014_tidy/1850_01_m014_tsurf"  # directory where your RDS files are located
outdir_ref <- tmpdir  # Assuming output directory is same as input directory

# List all RDS files in the directory
rds_files <- list.files(tmpdir, pattern = "\\.rds$", full.names = TRUE)

# List of already processed files (replace with the actual filenames)
processed_files <- c(
  ""
)

# Exclude already processed files
remaining_files <- rds_files[!basename(rds_files) %in% basename(processed_files)]


# # Define the reference time
# reference_time <- as.POSIXct("2024-11-19 16:48", tz = "CET")
#
# # Function to check if a file was created or modified before the reference time
# needs_processing <- function(file_path) {
#   # Check if the file modification time is before the reference time
#   return(file.info(file_path)$mtime < reference_time)
# }
#
# # Identify files that need processing
# remaining_files <- rds_files[sapply(rds_files, needs_processing)]

# Function to process and save each RDS file
process_file <- function(file_path) {
  df <- readRDS(file_path)

  # Apply the datetime fix
  df2 <- df |>
    dplyr::mutate(data = purrr::map(data, function(x){
      x |>
        tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
        dplyr::mutate(date = lubridate::ymd(date)) |>
        dplyr::select(-fract_day)
    }))

  # Save the processed file
  saveRDS(df2, file_path)  # Overwrite the file with the fixed datetime format
}

# Loop through the remaining files and process them
purrr::walk(remaining_files, process_file)





# # ## Net Radiation - monthly resolution -------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "netradiation";
set_name <- "m014_1850_1"
filnam <- list.files(
  paste0(path_ModESim, "/", varnam, "/", set_name),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1850_01_m014_netrad")
prefix <- "set1850_1_m014_netrad"

# for files that do not contain a date variable:
# define a function that derives the necessary dates from the filename and pass that function to fgetdate
# see ?map2tidy
# see example: https://github.com/geco-bern/grsofun/blob/084cbdc3c1c5094f3b43e0869f898abdcb973ff8/R/grsofun_tidy.R#L89-L96
library(dplyr)
library(purrr)
filename_to_monthly_datelist <- function(filename){
  # e.g. filename <- "~/Downloads/set1850_1_m001_precip_1850.nc"
  # year <- as.numeric(gsub(".*_([0-9]*).nc","\\1", basename(filename)))
  year_str <- gsub(".*_([0-9]+)_mon.netrad.nc","\\1", basename(filename))
  return(sprintf("%s-%02d", year_str, c(1:12)))
}

#convert to tidy
res_nr <- map2tidy(
  nclist = filnam[1:160],
  varnam = "netrad",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  do_chunks = TRUE, ncores = 1, fgetdate = filename_to_monthly_datelist,
  outdir = output_dir, fileprefix = "set1850_1_m014_netrad",
  overwrite = FALSE
 # filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

# # # Surface Pressure - monthly resolution ----------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "surfaceP";
set_name <- "m014_1850_1"
filnam <- list.files(
  paste0(path_ModESim, "/", varnam, "/", set_name),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1850_01_m014_patm")


# for files that do not contain a date variable:
# define a function that derives the necessary dates from the filename and pass that function to fgetdate
# see ?map2tidy
# see example: https://github.com/geco-bern/grsofun/blob/084cbdc3c1c5094f3b43e0869f898abdcb973ff8/R/grsofun_tidy.R#L89-L96
library(dplyr)
library(purrr)
filename_to_monthly_datelist <- function(filename){
  year_str <- gsub(".*_([0-9]+)_mon.patm.nc","\\1", basename(filename))
  return(sprintf("%s-%02d", year_str, c(1:12)))
}

prefix <- "set1850_1_m014_patm"
# convert to tidy
res_patm <- map2tidy(
  nclist = filnam[1:160],
  varnam = "patm",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  fgetdate = filename_to_monthly_datelist,
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1850_1_m014_patm",
  ncores = 1,
  overwrite = FALSE
 # filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)
#
# # #   test1 <- readRDS("~/scratch2/tidy/1850_01_m001_patm/set1850_1_m001_patm_LON_-120.000.rds")
# # #   test1$data

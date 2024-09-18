library(map2tidy)
library(dplyr)
library(stringr)
library(tidyr)

# list demo file path
# adjust path to where your ModE-Sim data is located
outdir <- "~/scratch2/tidy"

## Precipitation - daily resolution--------------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "precip";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_precip")


  prefix <- "set1420_1_m001_precip"
# convert to tidy
res_pr <- map2tidy(
  nclist = filnam[1:431],
  varnam = "precip",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1420_1_m001_precip",
  ncores = 1,
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
  )

# test <- readRDS("~/scratch2/tidy/1420_01_m001_precip/set1420_1_m001_precip_LON_-120.000.rds")
#
# test %>% slice(1) %>% unnest(data) %>%
#   tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
#   mutate(datetime = lubridate::ymd(datetime))

# Get a list of RDS files in the directory
file_list <- list.files(paste0(outdir, "/", "1420_01_m001_precip"), pattern = "\\.rds$", full.names = TRUE)


# # Loop through each file
# for (file in file_list) {
#   # Read the RDS file
#   test <- readRDS(file)
#
#   # Modify the datetime format
#   test_modified <- test %>%
#     slice(1) %>%
#     unnest(data) %>%
#     tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
#     mutate(datetime = lubridate::ymd(datetime))
#
#   # Save the modified data back to the RDS file, overwriting the original
#   saveRDS(test_modified, file)
# }

# list.files(tmpdir)
df <- readRDS(file.path(outdir, "/", "1420_01_m001_precip", "set1420_1_m001_precip_LON_-121.875.rds"))

# # test snippet to develop the separate and mutate command
# df |> dplyr::slice(1) |> tidyr::unnest(data) |>
#   tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
#   dplyr::mutate(date = lubridate::ymd(date))

# apply it to all nested data.frames using purrr::map
df2 <- df |> dplyr::mutate(data = purrr::map(data, function(x){
  x |>
    tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
    dplyr::mutate(date = lubridate::ymd(date)) |> dplyr::select(-fract_day)
}))

saveRDS(df2, file.path(outdir, "/", "1420_01_m001_precip", "set1420_1_m001_precip_LON_-121.875.rds"))


# # check result for the first element:
# df2 |> dplyr::slice(1) |> tidyr::unnest(data)

## Temperature - daily resolution-------------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "tsurf";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_tsurf")


prefix <- "set1420_1_m001_tsurf"
# convert to tidy
res_ts <- map2tidy(
  nclist = filnam[1:431],
  varnam = "tsurf",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1420_1_m001_tsurf",
  ncores = 1,
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

# test <- readRDS("~/scratch2/tidy/1420_01_m001_precip/set1420_1_m001_precip_LON_-120.000.rds")
#
# test %>% slice(1) %>% unnest(data) %>%
#   tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
#   mutate(datetime = lubridate::ymd(datetime))

# Get a list of RDS files in the directory
#file_list <- list.files(paste0(outdir, "/", "1420_01_m001_tsurf"), pattern = "\\.rds$", full.names = TRUE)

# # Loop through each file
# for (file in file_list) {
#   # Read the RDS file
#   test <- readRDS(file)
#
#   # Modify the datetime format
#   test_modified <- test %>%
#     slice(1) %>%
#     unnest(data) %>%
#     tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
#     mutate(datetime = lubridate::ymd(datetime))
#
#   # Save the modified data back to the RDS file, overwriting the original
#   saveRDS(test_modified, file)
# }
#
# test1 <- readRDS("~/scratch2/tidy/1420_01_m001_tsurf/set1420_1_m001_tsurf_LON_-121.875.rds")
# # Check if any unsuccessful:
# stopifnot(nrow(res_ts |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)
#test1$data

# list.files(tmpdir)
df <- readRDS(file.path(outdir, "/", "1420_01_m001_tsurf", "set1420_1_m001_tsurf_LON_-120.000.rds"))

# # test snippet to develop the separate and mutate command
# df |> dplyr::slice(1) |> tidyr::unnest(data) |>
#   tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
#   dplyr::mutate(date = lubridate::ymd(date))

# apply it to all nested data.frames using purrr::map
df2 <- df |> dplyr::mutate(data = purrr::map(data, function(x){
  x |>
    tidyr::separate(datetime, sep = "\\.", into = c('date', 'fract_day')) |>
    dplyr::mutate(date = lubridate::ymd(date)) |> dplyr::select(-fract_day)
}))

saveRDS(df2, file.path(outdir, "/", "1420_01_m001_tsurf", "set1420_1_m001_tsurf_LON_-120.000.rds"))




## Net Radiation - monthly resolution -------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "netradiation";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_netrad")
prefix <- "set1420_1_m001_netrad"

# for files that do not contain a date variable:
# define a function that derives the necessary dates from the filename and pass that function to fgetdate
# see ?map2tidy
# see example: https://github.com/geco-bern/grsofun/blob/084cbdc3c1c5094f3b43e0869f898abdcb973ff8/R/grsofun_tidy.R#L89-L96
library(dplyr)
library(purrr)
filename_to_monthly_datelist <- function(filename){
  # e.g. filename <- "~/Downloads/set1420_1_m001_precip_1420.nc"
  # year <- as.numeric(gsub(".*_([0-9]*).nc","\\1", basename(filename)))
  year_str <- gsub(".*_([0-9]+)_mon.netrad.nc","\\1", basename(filename))
  return(sprintf("%s-%02d", year_str, c(1:12)))
}

# # Create a list of date lists using lapply
# date_lists <- lapply(filnam[1:2], filename_to_monthly_datelist)

res_nr <- map2tidy(
  nclist = filnam[1:431],
  varnam = "netrad",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  do_chunks = TRUE, ncores = 1, fgetdate = filename_to_monthly_datelist,
  outdir = output_dir, fileprefix = "set1420_1_m001_netrad",
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

# # convert to tidy
# res_nr <- map2tidy(
#   nclist = filnam[1:431],
#   varnam = "netrad",
#   lonnam = "longitude",
#   latnam = "latitude",
#   timenam = "Time",
#   do_chunks = TRUE,
#   outdir = output_dir,
#   fileprefix = "set1420_1_m001_netrad",
#   ncores = 1,
#   overwrite = FALSE,
#   filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
# )
#
 test1 <- readRDS("~/scratch2/tidy/1420_01_m001_netrad/set1420_1_m001_netrad_LON_-120.000.rds")
# test1$data

## Surface Pressure - monthly resolution ----------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "surfaceP";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_patm")


# for files that do not contain a date variable:
# define a function that derives the necessary dates from the filename and pass that function to fgetdate
# see ?map2tidy
# see example: https://github.com/geco-bern/grsofun/blob/084cbdc3c1c5094f3b43e0869f898abdcb973ff8/R/grsofun_tidy.R#L89-L96
library(dplyr)
library(purrr)
filename_to_monthly_datelist <- function(filename){
  # e.g. filename <- "~/Downloads/set1420_1_m001_precip_1420.nc"
  # year <- as.numeric(gsub(".*_([0-9]*).nc","\\1", basename(filename)))
  year_str <- gsub(".*_([0-9]+)_mon.patm.nc","\\1", basename(filename))
  return(sprintf("%s-%02d", year_str, c(1:12)))
}

prefix <- "set1420_1_m001_patm"
# convert to tidy
res_patm <- map2tidy(
  nclist = filnam[1:431],
  varnam = "patm",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  fgetdate = filename_to_monthly_datelist,
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1420_1_m001_patm",
  ncores = 1,
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

# test1 <- readRDS("~/scratch2/tidy/1420_01_m001_patm/set1420_1_m001_patm_LON_-120.000.rds")
# test1$data

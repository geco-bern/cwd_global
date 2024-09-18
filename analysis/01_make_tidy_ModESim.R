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

# Loop through each file
for (file in file_list) {
  # Read the RDS file
  test <- readRDS(file)

  # Modify the datetime format
  test_modified <- test %>%
    slice(1) %>%
    unnest(data) %>%
    tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
    mutate(datetime = lubridate::ymd(datetime))

  # Save the modified data back to the RDS file, overwriting the original
  saveRDS(test_modified, file)
}

# Check if any unsuccessful:
stopifnot(nrow(res_pr |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


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
file_list <- list.files(paste0(outdir, "/", "1420_01_m001_tsurf"), pattern = "\\.rds$", full.names = TRUE)

# Loop through each file
for (file in file_list) {
  # Read the RDS file
  test <- readRDS(file)

  # Modify the datetime format
  test_modified <- test %>%
    slice(1) %>%
    unnest(data) %>%
    tidyr::separate(datetime, into = c("datetime", "fractional_day"), sep = "\\.") %>%
    mutate(datetime = lubridate::ymd(datetime))

  # Save the modified data back to the RDS file, overwriting the original
  saveRDS(test_modified, file)
}

test1 <- readRDS("~/scratch2/tidy/1420_01_m001_precip/set1420_1_m001_precip_LON_-120.000.rds")
# Check if any unsuccessful:
stopifnot(nrow(res_ts |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


## Net Radiation - monthly resolution -------------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "netradiation";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_netrad")


prefix <- "set1420_1_m001_netrad"
# convert to tidy
res_nr <- map2tidy(
  nclist = filnam[1:431],
  varnam = "netrad",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1420_1_m001_netrad",
  ncores = 1,
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

test1 <- readRDS("~/scratch2/tidy/1420_01_m001_netrad/set1420_1_m001_netrad_LON_-120.000.rds")
test1$data

## Surface Pressure - monthly resolution ----------------------------------------
path_ModESim <- "~/scratch2"
varnam <- "surfaceP";
filnam <- list.files(
  paste0(path_ModESim, "/", varnam),
  pattern = ".nc", full.names = TRUE)
output_dir = file.path(outdir, "1420_01_m001_patm")


prefix <- "set1420_1_m001_patm"
# convert to tidy
res_patm <- map2tidy(
  nclist = filnam[1:431],
  varnam = "patm",
  lonnam = "longitude",
  latnam = "latitude",
  timenam = "Time",
  do_chunks = TRUE,
  outdir = output_dir,
  fileprefix = "set1420_1_m001_patm",
  ncores = 1,
  overwrite = FALSE,
  filter_lon_between_degrees = c(-122, -120) #longitude of US-Ton: -120.9660
)

test1 <- readRDS("~/scratch2/tidy/1420_01_m001_patm/set1420_1_m001_patm_LON_-120.000.rds")
test1$data

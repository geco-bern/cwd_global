library(map2tidy)
library(dplyr)
library(stringr)

# list demo file path
# adjust path to where your ModE-Sim data is located
path_ModESim <- "~/scratch2/precip"

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
varnam <- "tas"; res <- "day"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_tas <- map2tidy(
  nclist = filnam,
  varnam = "tas",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_tas"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_tas |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


## Net Radiation - monthly resolution -------------------------------------------

varnam <- "rlus"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rlus <- map2tidy(
  nclist = filnam,
  varnam = "rlus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rlus"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rlus |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)



## Surface Pressure - monthly resolution ----------------------------------------
varnam <- "rsus"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rsus <- map2tidy(
  nclist = filnam,
  varnam = "rsus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rsus"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rsus |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


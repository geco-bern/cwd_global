#!/usr/bin/env Rscript

# script is called without any arguments

# Example:
# >./collect_cwd_annmax.R

library(dplyr)
library(map2tidy)
library(multidplyr)

indir_pcwd     <- "/data_1/CMIP6/tidy/pcwd_reset/test/"
indir_cwd      <- "/data_1/CMIP6/tidy/cwd_reset/test/"
# indir       <- "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/"
outfile_cwd  <- "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/act_evspsbl_cum_ANNMAX.nc" # adjust path to where the file should be written to
outfile_pcwd <- "/data_2/scratch/fbernhard/CMIP6/tidy/pcwd_reset/test/pot_evspsbl_cum_ANNMAX" # adjust path to where the file should be written to

# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd <- list.files(
  indir_pcwd,
  pattern = "^pcwd_[0-9]*_ANNMAX.rds$", # make sure to include only _ANNMAX.rds
  # pattern = "CWD_result_LON_[0-9.+-]*_ANNMAX.rds", # make sure to include only _ANNMAX.rds # TODO: change filename with map2tidy update
  full.names = TRUE
)
filnams_cwd <- list.files(
  indir_cwd,
  pattern = "^cwd_[0-9]*_ANNMAX.rds$", # make sure to include only _ANNMAX.rds
  # pattern = "CWD_result_LON_[0-9.+-]*_ANNMAX.rds", # make sure to include only _ANNMAX.rds # TODO: change filename with map2tidy update
  full.names = TRUE
)

if (length(filnams_pcwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams_pcwd), ".")
}
if (length(filnams_cwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams_cwd), ".")
}

# 3) Process files --------------------------------------------------------
df_pcwd <- lapply(filnams_pcwd,
              function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()
df_cwd <- lapply(filnams_cwd,
                  function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

# TODO: generate subfolder of outfile_pcwd and outfile_cwd
dir.create(dirname(outfile_pcwd), showWarnings = FALSE, recursive = TRUE)
readr::write_rds(
  df_pcwd,
  paste0(outfile_pcwd, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, "_ANNMAX.rds"))
readr::write_rds(
  df_cwd,
  paste0(outfile_cwd, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, "_ANNMAX.rds"))



# 4) Output to single, global NetCDF file ---------------------------------
# TODO(fabian): add another netCDF output for actual evspsbl
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

# create object that can be used with write_nc2()
df_pcwd <- df_pcwd |>
  select(lon, lat, year, max_deficit) |>
  arrange(year, lat, lon)

arr <- array(
  unlist(df_pcwd$max_deficit),
  dim = c(
    length(unique(df_pcwd$lon)),
    length(unique(df_pcwd$lat)),
    length(unique(df_pcwd$year))
  )
)

# image(arr[,,1])

# create object for use in rgeco::write_nc2()
obj <- list(
  lon = sort(unique(df_pcwd$lon)),
  lat = sort(unique(df_pcwd$lat)),
  time = lubridate::ymd(
    paste0(
      sort(unique(df_pcwd$year)),
      "-01-01"   # taking first of January as a mid-point for each year
    )
  ),
  vars = list(pot_evspsbl_cum = arr)
)


# Get meta information on code executed:
# gitrepo_hash = system("git rev-parse HEAD", intern=TRUE)
gitrepo_hash = system("git rev-parse --short HEAD", intern=TRUE)
gitrepo_status <-
  ifelse(system("git status --porcelain | wc -l", intern = TRUE) == "0",
         "",  #-clean-repository
         "-dirty-repository")
gitrepo_id <- paste0(
  "https://github.com/geco-bern/cwd_global@",
  gitrepo_hash, gitrepo_status)

# Write NetCDF file:
rgeco::write_nc2(
  obj,
  varnams = "pot_evspsbl_cum",
  make_tdim = TRUE,
  path = paste0(outfile_pcwd, ".nc"),
  units_time = "days since 2001-01-01",
  att_title      = "Global Potential Cumulative Water Deficit",
  att_history    = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), gitrepo_id, indir_pcwd)
)





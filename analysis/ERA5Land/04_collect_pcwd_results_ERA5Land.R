#!/usr/bin/env Rscript
# script is called without any arguments

# Example:
# > Rscript 04_collect_pcwd_results_ERA5Land.R

# When reloading re-install packages from renv.lock by doing:
library(renv)
source("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/renv/activate.R") # somewhat on UBELIX this needs to be done manually
renv::project()
options(renv.config.pak.enabled = FALSE)
renv::restore(prompt = FALSE)

library(dplyr)
library(map2tidy)

# first run indir        <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/03_pcwd_ANNMAX"
# first run outfile_pcwd <- "/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/04_result/PCWD_ANNMAX" # adjust path to where the file should be written to
indir        <- "/storage/scratch/giub_geco/fbernhard/ERA5LandLowRes_2025-2026/03_pcwd_ANNMAX"
outfile_pcwd <- "/storage/scratch/giub_geco/fbernhard/ERA5LandLowRes_2025-2026/04_result/PCWD_ANNMAX"

dir.create(dirname(outfile_pcwd), showWarnings = FALSE, recursive = TRUE)

# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd <- list.files(indir, pattern = "ERA5Land_pcwd_(LON_[0-9.+-]*)_ANNMAX.rds", full.names = TRUE)

if (length(filnams_pcwd) <= 1){
  stop("Should find multiple files. Only found " ,length(filnams_pcwd), ".")
}

# 3) Process files --------------------------------------------------------
df_pcwd <- lapply(filnams_pcwd,
              function(filnam) {readr::read_rds(filnam) |> tidyr::unnest(data)}) |>
  bind_rows()

dir.create(dirname(outfile_pcwd), showWarnings = FALSE, recursive = TRUE)
readr::write_rds(
  df_pcwd,
  paste0(outfile_pcwd, ".rds"), compress = "xz") # file.path(indir,paste0(fileprefix, "_ANNMAX.rds"))



# 4) Output to global NetCDF file ---------------------------------
prepare_write_nc2 <- function(df_cwd, varname){
  # create object that can be used with write_nc2()
  df_cwd <- df_cwd |>
    dplyr::select(lon, lat, year, max_deficit) |>
    arrange(year, lat, lon)

  arr <- array(
    unlist(df_cwd$max_deficit),
    dim = c(
      length(unique(df_cwd$lon)),
      length(unique(df_cwd$lat)),
      length(unique(df_cwd$year))
    )
  )

  # image(arr[,,1])

  # create object for use in rgeco_write_nc2()
  vars_list = list(arr)
  names(vars_list) <- varname

  obj <- list(
    lon = sort(unique(df_cwd$lon)),
    lat = sort(unique(df_cwd$lat)),
    time = lubridate::ymd(
      paste0(
        sort(unique(df_cwd$year)),
        "-01-01"   # taking first of January as a mid-point for each year
      )
    ),
    vars = vars_list
  )

  return(obj)
}

obj_pcwd <- prepare_write_nc2(df_pcwd, varname="pcwd_annmax")

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
source("../../R/ERA5Land-fullResNoNA/rgeco_write_nc2.R") # loads rgeco_write_nc2() # TODO: add modifications to rgeco package
# rgeco::write_nc2( # NOTE: this doesn't support (yet) arrays with more than 2^31 - 1 elements
rgeco_write_nc2(    # NOTE: this supports arrays with more than 2^31 - 1 elements
  obj_pcwd,
  varnams = "pcwd_annmax",
  make_tdim = TRUE,
  path = paste0(outfile_pcwd, ".nc"),
  units_time = "days since 2001-01-01",
  att_title      = "Global Potential Cumulative Water Deficit for ERA5Land data",
  att_history    = sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), get_repo_info(), indir)
)





# # Check output:
# options("bitmapType" = "cairo")
# library(tidync)
# library(ggplot2)
# #df_check <- tidync::tidync(paste0(outfile_pcwd, ".nc"))
# #df_check <- tidync::tidync("/storage/scratch/giub_geco/fbernhard/ERA5LandLowRes_2025-2026/04_result/PCWD_ANNMAX.nc")
# # library(ggplot2)
# df_check |> tidync::hyper_filter(lat = lat >= 40 & lat <= 50,
# )
# df <- df_check |>
#   tidync::hyper_filter(lat = lat >= 40 & lat <= 50,
#                         lon = lon >= 5 & lon <= 8) |>
#   tidync::hyper_tibble(drop=FALSE, na.rm = FALSE) |>
#   dplyr::mutate(time = lubridate::as_date(time),
#                 latlon = sprintf("%+06.1fN, %+06.1fE", as.numeric(lat), as.numeric(lon)))
# df |> print(n=Inf)
# ggplot(df, aes(x=time, y=pcwd_mm, color = latlon)) + geom_line()

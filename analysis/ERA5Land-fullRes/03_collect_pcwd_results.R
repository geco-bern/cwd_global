#!/usr/bin/env Rscript

### NOTE: status 2026-02-23: unsuccessful running of thsi script.
###       resulted in error messages (see. e.g slurm-45089938_2017.out):
###       
###       Error in `vec_rbind()`:
###       ! Negative `n` in `compact_rep()`.
###       ℹ In file 'utils.c' at line 897.
###       ℹ This is an internal error that was detected in the vctrs package.
###         Please report it at <https://github.com/r-lib/vctrs/issues> with a reprex (<https://tidyverse.org/help/>) and the full backtrace.
###       Backtrace:
###           ▆
###        1. ├─dplyr::bind_rows(...)
###        2. │ └─vctrs::vec_rbind(!!!dots, .names_to = .id, .error_call = current_env())
###        3. └─rlang:::stop_internal_c_lib(...)
###        4.   └─rlang::abort(message, call = call, .internal = TRUE, .frame = frame)
###       Execution halted
###       Finished on: 2026-02-09 06:45:38+01:00
###       
###       FB: This error appeared after ~30h when running for 1 year.
###       FB: Note that it might be linked that *.rds files were incomplete (around 10 were missing when this script was run)

# script is called with one arguments for parallelization:
# 1. year to extract

# Example:
# >./collect_pcwd_restults.R 2024

# # When using this script directly from RStudio, not from the shell, specify
# args <- 2022

# to receive arguments to script from the shell
args = commandArgs(trailingOnly=TRUE)
stopifnot(length(args)==1)

curr_year <- as.integer(args[1])


# options(repos = c(CRAN = "https://cloud.r-project.org"))
# install.packages(c("map2tidy", "dplyr", "stringr", "purrr", "ncdf4"))
# install.packages(c("gtable", "farver", "RColorBrewer")) # TODO: These trigger errors if missing when library(rgeco)
# devtools::install_github("geco-bern/rgeco")
library(rgeco)  # get it from https://github.com/geco-bern/rgeco

library(dplyr)
library(map2tidy)
library(multidplyr)

indir <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd"
outfile_pcwd <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd/03_daily_pcwd_YYYY.nc" # adjust path to where the file should be written to

# 3600 LON slices (for 1 year) use (`seff 45089938_2021`) 83GB memory  runtime 39h    output NetCDF: XMB
# 3600 LON slices (for 1 year) use (`seff 45089938_2024`) xGB memory  runtime XXh    output NetCDF: XMB
# 3600 LON slices (for 1 year) predicted                  xGB memory  runtime 38h    output NetCDF: 9500MB

# 128 LON slices (for 1 year) use (`seff 45047351_1956`) 20GB memory  runtime 96min  output NetCDF: 320MB
# 128 LON slices (for 1 year) predicted                 192GB memory  runtime 80min  output NetCDF: 336MB

# 16 LON slices (for 1 year) use (`seff 45047413_2022`)  13GB memory  runtime 11min  output NetCDF: 41MB # this is to see if by manually triggering gc() memory usage is further reduced
# 16 LON slices (for 1 year) use (`seff 45047380_2021`)  16GB memory  runtime 10min  output NetCDF: 41MB # ok good news that memory is not linear
# 16 LON slices (for 1 year) predicted                   24GB memory  runtime 10min  output NetCDF: 42MB

# 8 LON slices (for 1 year) use (`seff $jobid`) 12.2GB memory runtime 5min   output NetCDF: 21MB
# 4 LON slices (for 1 year) use (`seff $jobid`) 9.11GB memory runtime 3min   output NetCDF: 11MB
# 3 LON slices (for 1 year) use (`seff $jobid`) 7.21GB memory runtime 2min   output NetCDF: 7.6MB
# 2 LON slices (for 1 year) use (`seff $jobid`) 6GB memory    runtime 1.3min output NetCDF: 5MB


dir.create(dirname(outfile_pcwd), showWarnings = FALSE)

# 1) Define filenames of files to collect:  -------------------------------
filnams_pcwd <- list.files(indir, pattern = "ERA5Land_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)

# 1b) Make a simple plot of area around Bern:  -------------------------------

# library(ggplot2)
# library(readr)
# library(dplyr)
# rds_name <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd//ERA5Land_pcwd_LON_+007.400.rds"
# curr_year <- 2021
# full_tidy_slice <- readRDS(rds_name)
# temp <- full_tidy_slice |>
#   dplyr::filter(lat > 46, lat < 47) |> # slice(1) |>
#   # pcwd generated nested lists with elements 'inst' and 'df'. We only use df
#   tidyr::unnest_wider(data) |> select(-inst) |> select(lon, lat, df) |>
#   tidyr::unnest(df)
# temp2 <- temp |>
#   # subset year
#   dplyr::mutate(year = lubridate::year(date)) |>
#   dplyr::filter(year == !!curr_year)
#
# pl1 <- temp2 |>
#   # aggregate per region
#   mutate(region = "Bern") |>
#   select(lon, lat, date, region, deficit) |>
#   group_by(region, date) |>
#   summarise(#avg_pcwd = mean(deficit),
#             p50_pcwd = quantile(deficit, 0.50),
#             p25_pcwd = quantile(deficit, 0.25),
#             p75_pcwd = quantile(deficit, 0.75)) |>
#   # plot daily values of median and IQR across region:
#   ggplot(mapping = aes(x=date)) +
#   geom_ribbon(aes(ymin = p25_pcwd, ymax = p75_pcwd), alpha = 0.3) +
#   geom_line(aes(y = p50_pcwd)) +
#   labs(x=NULL, y="PCWD (mm)") +
#   scale_x_date(date_breaks = "3 months", date_minor_breaks = "1 month") +
#   theme_bw()
# pl2 <- pl1 + coord_cartesian(xlim = as.Date("2021-06-01") + c(0, 51)) +
#   scale_x_date(date_breaks = "1 month", date_minor_breaks = "1 day")
# ggsave(pl1, filename = "PCWD_Bern_2021.png", height = 2, width = 7.2, units = "in", dpi = 300)
# ggsave(pl2, filename = "PCWD_Bern_2021-june-51days.png", height = 2, width = 7.2, units = "in", dpi = 300)
# readr::write_csv(pl1$data, "PCWD_Bern_2021.csv")


# 2) Process 1st file  (Bern file) --------------------------------------------------------
# readRDS(filnams_pcwd[1])

# For development: Use Bern coordinates 46.947687535597794, 7.441952632324079
# basename(filnams_pcwd)
# basename(filnams_pcwd[75])

# rds_name <- filnams_pcwd[75]
# curr_year <- args
subset_pcwd_for_year <- function(rds_name, curr_year_arg){
  full_tidy_slice <- readRDS(rds_name)

  out <- full_tidy_slice |>
    # dplyr::filter(lat > 46, lat < 47) |> # slice(1) |>    # TODO: for development we also subset lat to be between 46 and 47 North
    # pcwd generated nested lists with elements 'inst' and 'df'. We only use df
    tidyr::unnest_wider(data) |> select(-inst) |> select(lon, lat, df) |>
    tidyr::unnest(df) |>
    # subset year
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::filter(year == !!curr_year_arg) |>
    select(lon, lat, date, pcwd_mm = deficit)

  # explicitly drop large intermediates and trigger GC to reduce RAM usage
  rm(full_tidy_slice)
  gc(FALSE)

  out
}

# 3) Process files --------------------------------------------------------
df_pcwd <- lapply(
  filnams_pcwd, # filnams_pcwd[75:(75+16-1)],   # TODO: remove this subsetting: and simply use filnams_pcwd
  function(filnam) subset_pcwd_for_year(filnam, curr_year_arg = curr_year)
  ) |>
  bind_rows()

# 4) Output to global NetCDF file ---------------------------------
# df_cwd <- df_pcwd
# varname <- "pcwd_mm"
prepare_write_nc2 <- function(df_cwd, varname){
  # create object that can be used with write_nc2()
  df_cwd <- df_cwd |>
    dplyr::select(lon, lat, date, pcwd_mm) |>
    arrange(date, lat, lon)

  arr <- array(
    unlist(df_cwd$pcwd_mm),
    dim = c(
      length(unique(df_cwd$lon)),
      length(unique(df_cwd$lat)),
      length(unique(df_cwd$date))
    )
  )
  # image(arr[,,1])

  # create object for use in rgeco::write_nc2()
  vars_list = list(arr)
  names(vars_list) <- varname

  obj <- list(
    lon = sort(unique(df_cwd$lon)),
    lat = sort(unique(df_cwd$lat)),
    time = sort(unique(df_cwd$date)),
    vars = vars_list
  )

  return(obj)
}

obj_pcwd <- prepare_write_nc2(df_pcwd, varname="pcwd_mm")

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
# get_repo_info()

# Write NetCDF file:

log_str <- sprintf(
    "Created on: %s, with R scripts from (%s) processing input data from: %s",
    Sys.Date(), get_repo_info(), indir)

rgeco::write_nc2(
  obj_pcwd,
  varnams     = "pcwd_mm",
  make_tdim   = TRUE,
  path        = gsub("YYYY", curr_year, outfile_pcwd),
  units_time  = "days since 2001-01-01",
  att_title   = "Potential Cumulative Water Deficit for ERA5Land data",
  att_history = log_str
)

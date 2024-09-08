#!/usr/bin/env Rscript

library(rgeco)  # get it from https://github.com/geco-bern/rgeco
library(dplyr)

indir <- "/data_1/CMIP6/tidy/pcwd_reset/test/"
fileprefix <- "pcwd"


df <- readr::read_rds(
  paste0(
    indir,
    fileprefix,
    "_ANNMAX.rds"
  )
)


# create object that can be used with write_nc2()
df <- df |>
  select(lon, lat, year, max_deficit) |>
  arrange(year, lat, lon)

arr <- array(
  unlist(df$max_deficit),
  dim = c(
    length(unique(df$lon)),
    length(unique(df$lat)),
    length(unique(df$year))
  )
)

# image(arr[,,1])

# create object for use in rgeco::write_nc2()
obj <- list(
  lon = sort(unique(df$lon)),
  lat = sort(unique(df$lat)),
  time = lubridate::ymd(
    paste0(
      sort(unique(df$year)),
      "-01-01"   # taking first of January as a mid-point for each year
    )
  ),
  vars = list(pot_evspsbl_cum = arr)
)

rgeco::write_nc2(
  obj,
  varnams = "pot_evspsbl_cum",
  make_tdim = TRUE,
  # adjust path to where the file should be written to
  path = "/data_1/CMIP6/tidy/pcwd_reset/test/pot_evspsbl_cum_ANNMAX.nc",
  units_time = "days since 2001-01-01"
)

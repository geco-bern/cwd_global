# Testing whether CMIP6 data is correctly read from files and handled.
# For Patricia Gribi

library(FluxDataKit)
library(tidyverse)
library(terra)
library(lubridate)

# specify test site
site <- "DE-Hai"

# get location
loc <- fdk_site_info %>%
  filter(sitename == site) %>%
  select(lon, lat)

## Read from original NetCDF files---------------

# load files
rasta_et <- rast("/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/evspsbl/mon/native/evspsbl_mon_CESM2_ssp585_r1i1p1f1_native.nc")
rasta_pr <- rast("/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/pr/day/native/pr_day_CESM2_ssp585_r1i1p1f1_native.nc")

# extract data
points <- vect(loc, geom = c("lon", "lat"), crs = "EPSG:4326")

# et in kg m-2 s-1 = mm s-1
vals_et <- extract(rasta_et, points, xy = FALSE, ID = FALSE, method = "simple")

# pr in kg m-2 s-1 = mm s-1
vals_pr <- extract(rasta_pr, points, xy = FALSE, ID = FALSE, method = "simple")

# wrangle nicely
df_et <- tibble(
  et = unlist(c(vals_et)) * 60 * 60 * 24,
  tstep = names(unlist(c(vals_et)))
) %>%
  mutate(tstep = as.numeric(stringr::str_remove(tstep, "evspsbl_"))) %>%

  # my interpretation - read correctly from time dimension
  mutate(date = seq(from = ymd("2015-01-15"), to = ymd("2100-12-15"), by = "months", )) %>%
  mutate(
    year = year(date),
    month = month(date)
  )

# 86 years of data
df_prec <-  tibble(
  # my interpretation - read correctly from time dimension
  date = seq(from = ymd("2015-01-01"), to = ymd("2100-12-31"), by = "days")
) %>%
  filter(!(month(date) == 2 & mday(date) == 29)) %>%
  mutate(
    pr = unlist(c(vals_pr)) * 60 * 60 * 24,
    tstep = names(unlist(c(vals_pr)))
  ) %>%
  mutate(tstep = as.numeric(stringr::str_remove(tstep, "pr_"))) %>%
  mutate(
    year = year(date),
    month = month(date)
    )

# combine
df <- df_prec %>%
  left_join(
    df_et %>%
      select(-date, -tstep),
    by = join_by(year, month)
  )

adf <- df %>%
  group_by(year) %>%
  summarise(
    et = sum(et),
    pr = sum(pr)
  ) %>%
  mutate(
    et_over_pr = et/pr
  )

meandf <- adf %>%
  summarise(
    et = sum(et),
    pr = sum(pr)
  ) %>%
  mutate(
    et_over_pr = et/pr
  )

adf %>%
  ggplot(aes(x = et_over_pr, y = ..density..)) +
  geom_histogram()

adf %>%
  pivot_longer(
    cols = c(et, pr),
    names_to = "type",
    values_to = "flux"
  ) %>%
  ggplot(aes(year, flux, color = type)) +
  geom_line()


## Read from tidy files---------------
filnam_et <- "/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_LON_+10.000.rds"
filnam_pr <- "/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/pr_day_CESM2_ssp585_r1i1p1f1_native_LON_+10.000.rds"

df_et <- readr::read_rds(filnam_et) %>%
  mutate(lat_int = as.integer(lat)) %>%
  filter(lat_int == 51)

df_pr <- readr::read_rds(filnam_pr) %>%
  mutate(lat_int = as.integer(lat)) %>%
  filter(lat_int == 51)

df_fromtidy <- df_pr %>%
  unnest(data) %>%
  mutate(date = ymd(stringr::str_sub(datetime, start = 1, end = 10))) %>%
  mutate(month = month(date), year = year(date)) %>%
  left_join(
    df_et %>%
      unnest(data) %>%
      mutate(date = ymd(stringr::str_sub(datetime, start = 1, end = 10))) %>%
      mutate(month = month(date), year = year(date)),
    join_by(lon, lat, lat_int, year, month)
  ) %>%
  select(lon, lat, month, year, pr, et = evspsbl) %>%
  mutate(
    pr = pr * 60 * 60 * 24,
    et = et * 60 * 60 * 24
  )

head(df %>% select(pr, et))
head(df_fromtidy %>% select(pr, et))

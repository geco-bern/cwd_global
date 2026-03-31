# 1b) Make a simple plot of area around Bern:  -------------------------------

library(ggplot2)
library(readr)
library(dplyr)
version <- "v2"
if (version == "v1") {
  rds_name <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd//ERA5Land_pcwd_LON_+007.400.rds"
  full_tidy_slice <- readRDS(rds_name)
  temp <- full_tidy_slice |>
    dplyr::filter(lat > 46, lat < 47) |> # slice(1) |>
    # pcwd generated nested lists with elements 'inst' and 'df'. We only use df
    tidyr::unnest_wider(data) |> select(-inst) |> select(lon, lat, df) |>
    tidyr::unnest(df) |> dplyr::rename(pcwd_mm = deficit)
} else if (version == "v2"){
  rds_name <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd_v2-doy-reset/ERA5Land_pcwd_LON_+007.400.rdsallyears_onlypcwd_mm.rds"
  full_tidy_slice <- readRDS(rds_name)
  temp <- full_tidy_slice |>
    dplyr::filter(lat > 46, lat < 47)
} else {
  rm(temp)
}

curr_year <- 2018
temp2 <- temp |>
  # subset year
  dplyr::mutate(year = lubridate::year(date)) |>
  dplyr::filter(year == !!curr_year)

pl0 <- ggplot(temp2, mapping = aes(x=date, y=pcwd_mm, group=lat)) +
  geom_line() +
  labs(x=NULL, y="PCWD (mm)") +
  scale_x_date(date_breaks = "3 months", date_minor_breaks = "1 month") +
  theme_bw() + facet_wrap(~lat)
pl0
pl0 + coord_cartesian(xlim=as.Date(c("2018-09-01","2018-12-31"))) +
  geom_point() +
  scale_x_date(date_breaks = "1 week") + theme(axis.text.x = element_text(angle=90))
pl1 <- temp2 |>
  # aggregate per region
  mutate(region = "Bern") |>
  select(lon, lat, date, region, pcwd_mm) |>
  group_by(region, date) |>
  summarise(#avg_pcwd = mean(pcwd_mm),
            p50_pcwd = quantile(pcwd_mm, 0.50),
            p25_pcwd = quantile(pcwd_mm, 0.25),
            p75_pcwd = quantile(pcwd_mm, 0.75)) |>
  # plot daily values of median and IQR across region:
  ggplot(mapping = aes(x=date)) +
  geom_ribbon(aes(ymin = p25_pcwd, ymax = p75_pcwd), alpha = 0.3) +
  geom_line(aes(y = p50_pcwd)) +
  labs(x=NULL, y="PCWD (mm)") +
  scale_x_date(date_breaks = "3 months", date_minor_breaks = "1 month") +
  theme_bw()
pl1
pl2 <- pl1 + coord_cartesian(xlim = as.Date("2018-06-01") + c(0, 51)) +
  scale_x_date(date_breaks = "1 month", date_minor_breaks = "1 day")

if (version == "v1"){
  ggsave(pl0, filename = "PCWD_Bern_2018_v1_raw.png", height = 2, width = 7.2, units = "in", dpi = 300)
  ggsave(pl1, filename = "PCWD_Bern_2018_v1.png", height = 2, width = 7.2, units = "in", dpi = 300)
  ggsave(pl2, filename = "PCWD_Bern_2018_v1-june-51days.png", height = 2, width = 7.2, units = "in", dpi = 300)
  readr::write_csv(pl1$data, "PCWD_Bern_2018_v1.csv")
} else if (version == "v2"){
  ggsave(pl0, filename = "PCWD_Bern_2018_v2_raw.png", height = 2, width = 7.2, units = "in", dpi = 300)
  ggsave(pl1, filename = "PCWD_Bern_2018_v2.png", height = 2, width = 7.2, units = "in", dpi = 300)
  ggsave(pl2, filename = "PCWD_Bern_2018_v2-june-51days.png", height = 2, width = 7.2, units = "in", dpi = 300)
  readr::write_csv(pl1$data, "PCWD_Bern_2018_v2.csv")
}


ds_nc <- tidync::tidync("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_v2-doy-reset_netcdf/data_derived_03_daily_pcwd_v2-doy_2019_r-generated.nc")

df_nc <- ds_nc |>
  tidync::hyper_filter(lon = index == 74,
                       lat = index >= 700+(460) & index <= 700+(469)) |>
  tidync::hyper_tibble()
df_nc <- df_nc |> mutate(time = lubridate::ymd(time))
ggplot(df_nc, aes(x=time, y=pcwd_mm, group=lat)) + geom_line() +
  facet_wrap(~lat)

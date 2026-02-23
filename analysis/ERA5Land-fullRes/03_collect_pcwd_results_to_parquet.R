#!/usr/bin/env Rscript

# script is called without any arguments

#options(repos = c(CRAN = "https://cloud.r-project.org"))
#install.packages(c("arrow"))
#install.packages(c("pbmcapply"))

library(arrow)
library(dplyr)
library(tidyr)
library(lubridate)
library(pbmcapply)

indir           <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd"
# indir           <- "/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/02_daily_pcwd/"
# outfile_parquet <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_fullsbatch_gc2.parquet" # adjust path to where the file should be written to
outfile_parquet <- sprintf("/scratch/local/%s/data_derived_03_daily_pcwd.parquet", Sys.getenv("SLURM_JOB_ID")) # adjust path to where the file should be written to

filnams_pcwd <- list.files(indir, pattern = "ERA5Land_pcwd_(LON_[0-9.+-]*).rds", full.names = TRUE)


filnams_to_loop <- filnams_pcwd
# filnams_to_loop <- list(
#   # "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+272.000.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+275.000.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+275.500.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+276.000.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+287.300.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+289.200.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+282.600.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+285.800.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+291.100.rds",
#   "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_02_daily_pcwd/ERA5Land_pcwd_LON_+295.700.rds"
# )
outfile_parquet <- "/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_rsynced.parquet"

# Partition by year and LON index into a standardized format:
# following: https://arrow.apache.org/docs/r/articles/dataset.html#writing-datasets
# outfile_parquet <- sprintf("/scratch/local/%s/data_derived_03_daily_pcwd.parquet", Sys.getenv("SLURM_JOB_ID")) # adjust path to where the file should be written to

res <- pbmclapply(
# lapply(
  filnams_to_loop, function(rds_name){
    LON_str <- gsub("ERA5Land_pcwd_(LON_[0-9.+-]*).rds", "\\1", basename(rds_name))
    # subset a specific year and only keep pcwd
    readRDS(rds_name) |>
      unnest_wider(data) |> select(-inst) |> unnest(df) |>
      mutate(year = year(date)) |>
      mutate(LON_str = LON_str) |>
      select(lon, lat, date, pcwd_mm = deficit, year, LON_str) |> # include partition column
      group_by(year, LON_str) |>
      write_dataset(outfile_parquet,
                    format = "parquet", # this appears to be compressed by default ('snappy')
                    # compression = "snappy",#"uncompressed",
                    max_partitions = 3600L*120L) # default is 1024L, but we already have 3600 LON
    gc(FALSE) # explicitly trigger GC to reduce RAM usage
    return(TRUE)
  # })
  }, mc.cores = 8) # 9 slices in 450sec with 4 cores => 6MB files
                    # 9 slices in 765sec with 8 cores => 6MB files
                    # 9 slices in 450sec with 32 cores => 6MB files

# res <- list(TRUE, TRUE)
write.csv2(
  data.frame(
    fname=filnams_to_loop,
    parquet_success = unlist(res)
  ),
  file = sprintf("slurm-%s_R-res.csv", Sys.getenv("SLURM_JOB_ID"))
)


# parquet_name <- gsub(".rds$", paste0("_lon=", round(unique(out$lon),2), ".parquet"), basename(rds_name))
# ds <- arrow::open_dataset("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_rsynced2.parquet/")
# ds <- arrow::open_dataset("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_full.parquet/")
# ds <- arrow::open_dataset("/scratch/local/45273463/data_derived_03_daily_pcwd.parquet")
# ds <- arrow::open_dataset("/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data_derived_03_daily_pcwd_rsynced.parquet/")
# ds |> select(LON_str) |> unique() |> collect()
# ds |> filter(year == 2021) |> filter(LON_str %in% c("LON_+007.400", "LON_+007.600")) |> collect()
# ds |> filter(year == 2021) |> filter(LON_str %in% c("LON_+007.400")) |> collect()
# ds |> filter(year == 2021) |> filter(LON_str %in% c("LON_+272.000")) |> collect()
# df <- ds |> filter(year == 2021) |> arrange(-lat) |> collect() |> filter(!is.na(pcwd_mm))
# library(ggplot2)
# df |> filter(date == "2021-01-01") |> ggplot(aes(x = lon, y=lat)) + geom_point()




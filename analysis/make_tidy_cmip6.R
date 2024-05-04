library(map2tidy)
library(dplyr)
library(ggplot2)

# list demo file path
path_cmip6 <- "~/data/cmip6-ng/"

## Evapotranspiration
varnam <- "evspsbl"
res <- "mon"
files <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc",
  full.names = TRUE
  )

# load and convert
df <- map2tidy(
  nclist = files,
  varnam = "pr",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "/data/scratch/CMIP6ng/cmip6_tidy/",
  fileprefix = "pr_day_CESM2_historical_r1i1p1f1_native"
)

## Precipitation ---------------
files <- list.files(path, pattern = "pr_day", full.names = TRUE)





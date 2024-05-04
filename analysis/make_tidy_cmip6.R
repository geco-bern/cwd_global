library(map2tidy)
library(dplyr)

# list demo file path
path <- "/data/scratch/CMIP6ng/cmip6-ng/pr/day/native/"

## Precipitation ---------------
files <- list.files(path, pattern = "pr_day", full.names = TRUE)

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
  fileprefix = "pr_day_CESM2_historical_r1i1p1f1_native_",
  ncores = 4     # number of cores
)



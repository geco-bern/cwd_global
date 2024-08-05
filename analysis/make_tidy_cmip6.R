library(map2tidy)
library(dplyr)
library(stringr)

# list demo file path
path_cmip6 <- "~/data/cmip6-ng/"

## Evapotranspiration -----------------
varnam <- "evspsbl"
res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc",
  full.names = TRUE
  )

if (length(filnam) != 1){
  stop("Should find only a single file.")
}

# load and convert
df <- map2tidy(
  nclist = filnam,
  varnam = "evspsbl",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  # timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/evspsbl/",
  fileprefix = str_remove(basename(filnam), ".nc")
  # single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

## Precipitation ---------------
varnam <- "pr"
res <- "day"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc",
  full.names = TRUE
)

if (length(filnam) != 1){
  stop("Should find only a single file.")
}

# load and convert
df <- map2tidy(
  nclist = filnam,
  varnam = "pr",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)


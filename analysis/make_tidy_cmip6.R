library(map2tidy)
library(dplyr)
library(stringr)

# list demo file path
path_cmip6 <- "~/data_download/CMIP6ng_download/cmip6-ng/"

## Evapotranspiration --------------------------------------------------------
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
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/evspsbl/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

## Precipitation -------------------------------------------------------------
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
  outdir = "~/data/cmip6-ng/tidy/pr/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

## Temperature ---------------------------------------------------------------
varnam <- "tas"
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
  varnam = "tas",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/tas/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

## Radiation -----------------------------------------------------------------

### up longwave radiation -------------------------
varnam <- "rlus"
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
  varnam = "rlus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/rlus/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

### down longwave radiation ----------------------
varnam <- "rlds"
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
  varnam = "rlds",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/rlds/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

### down shortwave radiation ----------------------
varnam <- "rsds"
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
  varnam = "rsds",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/rsds/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

### up shortwave radiation ------------------------
varnam <- "rsus"
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
  varnam = "rsus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/rsus/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

## Elevation -------------------------------------------------------------
varnam <- "elevation"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/"),
  pattern = ".nc",
  full.names = TRUE
)

if (length(filnam) != 1){
  stop("Should find only a single file.")
}

# load and convert
df <- map2tidy(
  nclist = filnam,
  varnam = "elevation",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  timedimnam = "time",
  do_chunks = TRUE,
  outdir = "~/data/cmip6-ng/tidy/elevation/",
  fileprefix = str_remove(basename(filnam), ".nc"),
  single_basedate = TRUE
  # ncores = 2  # parallel::detectCores()
)

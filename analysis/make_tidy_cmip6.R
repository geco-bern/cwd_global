library(map2tidy)
library(dplyr)
library(stringr)

# list demo file path
path_cmip6     <- "/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/"

outdir_evspsbl <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/evspsbl/"
outdir_pr      <- "/data_2/scratch/fbernhard/CMIP6ng_CESM2_ssp585/cmip6-ng/tidy/pr/"

## Evapotranspiration -----------------
varnam <- "evspsbl"
res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc",
  full.names = TRUE
  )

if (length(filnam) != 1){
  stop("Should find exactly one single file.")
}

# load and convert
res_evspsbl <- map2tidy(
  nclist = filnam,
  varnam = "evspsbl",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = outdir_evspsbl,
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 12,  # parallel::detectCores()
  overwrite = FALSE
)

# Check if any unsuccessful:
tidyr::unnest(res_evspsbl, data) |>
  filter(!grepl("Written",data))

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
res_pr <- map2tidy(
  nclist = filnam,
  varnam = "pr",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = outdir_pr,
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 12,  # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
tidyr::unnest(res_pr, data) |>
  filter(!grepl("Written",data))


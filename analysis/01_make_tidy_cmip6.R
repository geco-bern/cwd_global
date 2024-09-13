library(map2tidy)
library(dplyr)
library(stringr)

# list demo file path
# adjust path to where your cmip6 data is located
path_cmip6 <- "/data/scratch/CMIP6ng_CESM2_ssp585/cmip6-ng/"

outdir <- "/data_2/scratch/fbernhard/CMIP6/tidy/"

## Evapotranspiration --------------------------------------------------------
varnam <- "evspsbl"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find exactly one single file.")}

# convert to tidy
res_evspsbl <- map2tidy(
  nclist = filnam,
  varnam = "evspsbl",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_evspsbl"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,  # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_evspsbl |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)

## Precipitation -------------------------------------------------------------
varnam <- "pr"; res <- "day"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_pr <- map2tidy(
  nclist = filnam,
  varnam = "pr",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_pr"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_pr |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


## Temperature ---------------------------------------------------------------
varnam <- "tas"; res <- "day"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_tas <- map2tidy(
  nclist = filnam,
  varnam = "tas",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_tas"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_tas |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


## Radiation -----------------------------------------------------------------

### up longwave radiation -------------------------
varnam <- "rlus"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rlus <- map2tidy(
  nclist = filnam,
  varnam = "rlus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rlus"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rlus |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


### down longwave radiation ----------------------
varnam <- "rlds"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rlds <- map2tidy(
  nclist = filnam,
  varnam = "rlds",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rlds"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rlds |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


### down shortwave radiation ----------------------
varnam <- "rsds"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rsds <- map2tidy(
  nclist = filnam,
  varnam = "rsds",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rsds"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rsds |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


### up shortwave radiation ------------------------
varnam <- "rsus"; res <- "mon"
filnam <- list.files(
  paste0(path_cmip6, varnam, "/", res, "/native/"),
  pattern = ".nc", full.names = TRUE)
if (length(filnam) != 1){stop("Should find only a single file.")}

# convert to tidy
res_rsus <- map2tidy(
  nclist = filnam,
  varnam = "rsus",
  lonnam = "lon",
  latnam = "lat",
  timenam = "time",
  do_chunks = TRUE,
  outdir = file.path(outdir, "01_rsus"),
  fileprefix = str_remove(basename(filnam), ".nc"),
  ncores = 20,   # parallel::detectCores()
  overwrite = FALSE
)
# Check if any unsuccessful:
stopifnot(nrow(res_rsus |> tidyr::unnest(data) |> filter(!grepl("Written",data))) == 0)


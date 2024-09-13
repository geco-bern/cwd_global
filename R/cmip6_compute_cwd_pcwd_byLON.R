cmip6_compute_cwd_pcwd_byLON <- function(
    LON_string,
    indir,
    outdir){

  #############################################
  # Define hardcoded paths and hardcoded options:
  indir_evspsbl   <- file.path(indir, "01_evspsbl")
  indir_tas       <- file.path(indir, "01_tas")
  indir_prec      <- file.path(indir, "01_pr")
  indir_rlus      <- file.path(indir, "01_rlus")
  indir_rlds      <- file.path(indir, "01_rlds")
  indir_rsds      <- file.path(indir, "01_rsds")
  indir_rsus      <- file.path(indir, "01_rsus")
  # indir_elevation <- file.path(indir, "01_elevation")
  indir_elevation <- file.path("/data_1/CMIP6/tidy/", "elevation")

  path_cwd  <- file.path(outdir, paste0("CMIP6_cwd", "_", LON_string, ".rds"))
  path_pcwd <- file.path(outdir, paste0("CMIP6_pcwd", "_", LON_string, ".rds"))

  # prepare output names
  # stopifnot(grepl(pattern = "\\[LONSTRING\\]", outfilename_template))
  # outpath <- file.path(outdir, gsub("\\[LONSTRING\\]", LON_string, outfilename_template))
  #############################################


  print(paste0(Sys.time(), ", LON: ", LON_string))

  # load functions that will be applied to time series
  source(paste0(here::here(), "/R/get_cwd_withSnow_and_reset.R"))

  # read from files that contain tidy data for a single longitudinal band
  # read evapotranspiration file tidy
  filnam <- file.path(indir_evspsbl, paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_",
                                            LON_string,".rds"))
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- file.path(indir_prec, paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_",
                                         LON_string,".rds"))
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- file.path(indir_tas, paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_",
                                         LON_string,".rds"))
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- file.path(indir_rlus, paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_",
                                        LON_string,".rds"))
  df_rlus <- readr::read_rds(filnam)

  filnam <- file.path(indir_rlds, paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_",
                                         LON_string,".rds"))
  df_rlds <- readr::read_rds(filnam)

  filnam <- file.path(indir_rsds, paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_",
                                         LON_string,".rds"))
  df_rsds <- readr::read_rds(filnam)

  filnam <- file.path(indir_rsus, paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_",
                                         LON_string,".rds"))
  df_rsus <- readr::read_rds(filnam)


  # read elevation file and convert to data frame
  # library(terra)
  filnam <- file.path(indir_elevation, "elevation.nc")
  rasta_elevation <- terra::rast(filnam)

  ## read the needed longitude value and extract the latitude values
  lon <- df_prec[["lon"]][1]
  latitudes <- unique(df_prec$lat)

  ## create a data frame of the coordinates
  loc <- data.frame(lon = rep(lon, length(latitudes)), lat = latitudes)

  ## convert to vector points
  points <- terra::vect(loc, geom = c("lon", "lat"), crs = "EPSG:4326")

  ## extract values
  vals <- terra::extract(rasta_elevation, points, xy = FALSE, ID = FALSE, method = "simple")

  # combine coordinates with extracted values
  df_elevation <- data.frame(
    lon = lon,
    lat = latitudes,
    value = vals
  )

  ## set NA-value at 90 degrees to 0
  df_elevation[is.na(df_elevation)] <- 0


  # unnest all the data frames
  df_rsds <- df_rsds |> tidyr::unnest(data)
  df_rsus <- df_rsus |> tidyr::unnest(data)
  df_rlds <- df_rlds |> tidyr::unnest(data)
  df_rlus <- df_rlus |> tidyr::unnest(data)
  df_evap <- df_evap |> tidyr::unnest(data)
  df_prec <- df_prec |> tidyr::unnest(data) # lon lat pr time
  df_tas  <- df_tas  |> tidyr::unnest(data)


  # unit conversions
  ## precipitation
  df_prec <-  df_prec |>
    mutate(pr = pr * 86400 ) # conversion to mm day-1

  ## actual evapotranspiration
  df_evap <-  df_evap |>
    mutate(evspsbl = evspsbl * 86400 ) # conversion to mm day-1

  ## temperature
  df_tas <-  df_tas |>
    mutate(tas = tas - 273.15) # conversion to °C

  # data wrangling and time resolution adjustments

  ## extract year and month from the time column
  df_prec <- df_prec |>
    mutate(time = lubridate::ymd_hms(datetime)) |> select(-datetime) |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))

  df_tas <- df_tas |>
    mutate(time = lubridate::ymd_hms(datetime)) |> select(-datetime)

  df_evap <- df_evap |>
    mutate(time = lubridate::ymd_hms(datetime)) |>
    mutate(year = lubridate::year(time), month = lubridate::month(time)) |>
    select(-time, -datetime)

  ## compute net_radiation
  ### create new data frame
  df_radiation <- df_rsds |>
    left_join(df_rsus, by = join_by(lon, lat, datetime)) |>
    left_join(df_rlds, by = join_by(lon, lat, datetime)) |>
    left_join(df_rlus, by = join_by(lon, lat, datetime))
  ### calculate net radiation
  df_net_radiation <- df_radiation |>
    mutate(net_radiation = (rsds - rsus) + (rlds - rlus)) |>
    select(-rsds, -rsus, -rlds, -rlus)
  ## extract year and month from the time column
  df_net_radiation <- df_net_radiation |>
    mutate(time = lubridate::ymd_hms(datetime)) |>
    mutate(year = lubridate::year(time), month = lubridate::month(time)) |>
    select(-time, -datetime)

  ### merge all such that monthly data is repeated for each day within month
  ## cwd
  df_cwd <- df_prec |>
    left_join(df_evap, by = join_by(lon, lat, year, month)) |>
    left_join(df_tas, by = join_by(lon, lat, time)) |>
    dplyr::select(-year, -month)
  ## pcwd
  df_pcwd <- df_prec |>  # one of the daily data frames
    left_join(df_net_radiation, by = join_by(lon, lat, year, month)) |>
    left_join(df_tas, by = join_by(lon, lat, time)) |>
    left_join(df_elevation, by = join_by(lon, lat)) |>
    dplyr::select(-year, -month)

  # pet-calculation
  ## calculate surface pressure
  source(paste0(here::here(), "/R/calc_patm.R"))
  df_pcwd$patm <- calc_patm(df_pcwd$elevation)
  ## apply pet() function
  df_pcwd <- df_pcwd |>
    mutate(pet = 60 * 60 * 24 * cwd::pet(net_radiation, tas, patm)) # conversion from mm s-1 to mm day-1



  # out_cwd
  out_cwd <- df_cwd |>
    select(lon, lat, time, pr, tas, evspsbl) |>

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    tidyr::nest() |> dplyr::ungroup() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    ###slice(1:2)|> # uncomment for development/debugging
    mutate(data = purrr::map(data, ~get_cwd_withSnow_and_reset(.), .progress = TRUE))

  # write (complemented) data to cwd- and pcwd-files with meaningful name and index counter
  message(paste0("Writing file ", path_cwd , " ..."))
  readr::write_rds(out_cwd, path_cwd)

  # out pcwd
  out_pcwd <- df_pcwd |>
    select(lon, lat, time, pr, tas, evspsbl = pet) |> # Use POTENTIAL ET as ET estimate

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    group_by(lon, lat) |>
    tidyr::nest() |> dplyr::ungroup() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    ###slice(1:2)|> # uncomment for development/debugging
    mutate(data = purrr::map(data, ~get_cwd_withSnow_and_reset(.), .progress = TRUE))

  # write (complemented) data to cwd- and pcwd-files with meaningful name and index counter
  message(paste0("Writing file ", path_pcwd, " ..."))
  readr::write_rds(out_pcwd, path_pcwd)

  # don't return data - it's written to file
  return(NULL)
}

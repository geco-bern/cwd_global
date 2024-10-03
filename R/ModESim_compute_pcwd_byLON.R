#LON_string <- "LON_-120.000"
#TODO: have as function again
ModESim_compute_pcwd_byLON <- function(
    LON_string,
    indir,
    outdir){

  #############################################
  # Define hardcoded paths and hardcoded options: change year and set number to adapt for other sets
  indir_patm      <- file.path(indir, "1420_01_m001_patm")
  indir_tas       <- file.path(indir, "1420_01_m001_tsurf")
  indir_prec      <- file.path(indir, "1420_01_m001_precip")
  indir_netrad    <- file.path(indir, "1420_01_m001_netrad")

  path_pcwd <- file.path(outdir, paste0("ModESim_pcwd", "_", LON_string, ".rds"))

  # prepare output names
  # stopifnot(grepl(pattern = "\\[LONSTRING\\]", outfilename_template))
  # outpath <- file.path(outdir, gsub("\\[LONSTRING\\]", LON_string, outfilename_template))
  #############################################


  print(paste0(Sys.time(), ", LON: ", LON_string))

  # load functions that will be applied to time series
  source("~/cwd_global/R/get_cwd_withSnow_and_reset.R")

  # read from files that contain tidy data for a single longitudinal band
  # read surface Pressure file tidy
  filnam <- file.path(indir_patm, paste0("set1420_1_m001_patm_",
                                            LON_string,".rds"))
  df_patm <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- file.path(indir_prec, paste0("set1420_1_m001_precip_",
                                         LON_string,".rds"))
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- file.path(indir_tas, paste0("set1420_1_m001_tsurf_",
                                         LON_string,".rds"))
  df_tas <- readr::read_rds(filnam)


  # read net radiation file tidy
  filnam <- file.path(indir_netrad, paste0("set1420_1_m001_netrad_",
                                        LON_string,".rds"))
  df_net_radiation <- readr::read_rds(filnam)

  # unnest all the data frames
  df_net_radiation  <- df_net_radiation |> tidyr::unnest(data)
  df_patm           <- df_patm   |> tidyr::unnest(data)
  df_prec           <- df_prec |> tidyr::unnest(data) # lon lat pr time
  df_tas            <- df_tas  |> tidyr::unnest(data)


  # unit conversions
  ## precipitation; total precip has units of kg m-2 s-1
  df_prec <-  df_prec |>
    mutate(precip = precip * 86400 ) # conversion to mm day-1

  ## temperature
  df_tas <-  df_tas |>
    mutate(tsurf = tsurf - 273.15) # conversion to °C

  # data wrangling and time resolution adjustments

  df_tas <- df_tas |>
    mutate(date = lubridate::ymd(date)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date))

  df_prec <- df_prec |>
    mutate(date = lubridate::ymd(date)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date))

  df_patm <- df_patm |>
    mutate(date = lubridate::ym(datetime)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  df_net_radiation <- df_net_radiation |>
    mutate(date = lubridate::ym(datetime)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date)) |>
    dplyr::select(-datetime)


  ## merge all such that monthly data is repeated for each day within month
  # pcwd
  df_pcwd <- df_prec |>  # one of the daily data frames
    left_join(df_net_radiation, by = c("year", "month", "lon", "lat")) |>
    left_join(df_patm, by = c("lon", "lat", "year", "month")) |>
    left_join(df_tas, by = c("lon", "lat", "date")) |>
    dplyr::select(-date, -year.y, -month.y, -date.y,
                  year = year.x, month = month.x, date = date.x)

  # pet-calculation
  ## apply pet() function
  df_pcwd <- df_pcwd |>
    mutate(pet = 60 * 60 * 24 * cwd::pet(netrad, tsurf, patm)) # conversion from mm s-1 to mm day-1


  # out pcwd
out_pcwd <- df_pcwd |>
    dplyr::select(lon, lat, date, precip, tsurf, pet) |> # Use POTENTIAL ET as ET estimate

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

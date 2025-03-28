#LON_string <- "LON_+105.000"

ERA5Land_compute_pcwd_byLON <- function(
    LON_string,
    indir,
    outdir){

  #############################################
  # Define hardcoded paths and hardcoded options: change year and set number to adapt for other sets
  indir_prec      <- file.path(indir, "total_prec")
  indir_pev    <- file.path(indir, "total_pet")
  # prepare output names
  path_pcwd <- file.path(outdir, paste0("ERA5Land_pcwd", "_", LON_string, ".rds"))
  #############################################


  print(paste0(Sys.time(), ", LON: ", LON_string))

  # load functions that will be applied to time series
  source("/storage/homefs/ph23v078/cwd_global/R/get_cwd_withSnow_and_reset_ERA5Land.R")

  # read from files that contain tidy data for a single longitudinal band
  # read precipitation file tidy
  filnam <- file.path(indir_prec, paste0("ERA5Land_UTCDaily_tottp_",
                                         LON_string,".rds"))
  df_prec <- readr::read_rds(filnam)

  # read potential evaporation file tidy
  filnam <- file.path(indir_pev, paste0("ERA5Land_UTCDaily_totpev_",
                                        LON_string,".rds"))

  df_pev <- readr::read_rds(filnam)

  # unnest all the data frames
  df_prec           <- df_prec |> tidyr::unnest(data) # lon lat pr time
  df_pev            <- df_pev  |> tidyr::unnest(data)


  # unit conversions
  ## precipitation; total precip has units of m/day
  df_prec <-  df_prec |>
    mutate(precip = tot_tp * 1000 ) |> # conversion to mm/day
    dplyr::select(-tot_tp)

  ## evaportranspiration; total PET has units of m/day
  df_pev <-  df_pev |>
    mutate(pet = tot_pev * 1000 *-1) # conversion to mm/day and to positive values

  # data wrangling and time resolution adjustments
  df_prec <- df_prec |>
    mutate(date = lubridate::ymd(datetime)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date))

  df_pev <- df_pev |>
    mutate(date = lubridate::ymd(datetime)) |>
    mutate(year = lubridate::year(date), month = lubridate::month(date)) |>
    dplyr::select(-datetime) |>
    group_by(year, month, lon, lat) |>
    summarise(pet = mean(pet, na.rm = TRUE), .groups = "drop") |>
    mutate(date = as.Date(paste0(year, "-", month, "-01")))




  ## merge all such that monthly data is repeated for each day within month
  # pcwd
  df_pcwd <- df_prec |>
    left_join(df_pev, by = c("lon", "lat", "year", "month"), suffix = c("", ".pet")) |>
    dplyr::select(lon, lat, precip, datetime, date, year, month, pet)


  # # pet-calculation - not needed for ERA5Land output
  # ## apply pet() function
  # df_pcwd <- df_pcwd |>
  #   mutate(pet = 60 * 60 * 24 * cwd::pet(netrad, tsurf, patm)) # conversion from mm s-1 to mm day-1


  # out pcwd
out_pcwd <- df_pcwd |>
    dplyr::select(lon, lat, date, precip, pet) |> # Use POTENTIAL ET as ET estimate

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    group_by(lon, lat) |>
    tidyr::nest() |> dplyr::ungroup() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    ###slice(1:2)|> # uncomment for development/debugging
    mutate(data = purrr::map(data, ~get_cwd_withSnow_and_reset_ERA5Land(.), .progress = TRUE))
  # write (complemented) data to cwd- and pcwd-files with meaningful name and index counter
  message(paste0("Writing file ", path_pcwd, " ..."))
  readr::write_rds(out_pcwd, path_pcwd)

  # don't return data - it's written to file
  return(NULL)
}


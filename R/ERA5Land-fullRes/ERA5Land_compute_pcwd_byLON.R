#LON_string <- "LON_-071.250"

ERA5Land_compute_pcwd_byLON <- function(
    LON_string,
    indir,
    outdir){

  #############################################
  # Define hardcoded paths and hardcoded options: change year and set number to adapt for other sets
  indir_prec      <- file.path(indir, "tot_tp")
  #indir_pev    <- file.path(indir, "total_pet") # unused, pet is computed with cwd::pet()
  indir_patm      <- file.path(indir, "mean_sp")
  indir_tas       <- file.path(indir, "mean_t2m")
  # indir_netrad    <- file.path(indir, "netrad") # unused
  indir_str       <- file.path(indir, "tot_str")
  indir_ssr       <- file.path(indir, "tot_ssr")
  # prepare output names
  path_pcwd <- file.path(outdir, paste0("ERA5Land_pcwd", "_", LON_string, ".rds"))
  #############################################


  print(paste0(Sys.time(), ", LON: ", LON_string))

  # read from files that contain tidy data for a single longitudinal band
  # read precipitation file tidy
  filnam <- file.path(indir_prec, paste0("ERA5Land_UTCDaily_tot_tp_",
                                         LON_string,".rds"))
  df_precip <- readr::read_rds(filnam)

  # # read potential evaporation file tidy
  # filnam <- file.path(indir_pev, paste0("ERA5Land_UTCDaily_totpev_",
  #                                       LON_string,".rds"))
  #
  # df_pev <- readr::read_rds(filnam)

  # read temperature file tidy
  filnam <- file.path(indir_tas, paste0("ERA5Land_UTCDaily_mean_t2m_",
                                         LON_string,".rds"))
  df_tsurf  <- readr::read_rds(filnam)

  # read surface Pressure file tidy
  filnam <- file.path(indir_patm, paste0("ERA5Land_UTCDaily_mean_sp_",
                                         LON_string,".rds"))
  df_patm <- readr::read_rds(filnam)

  # read net radiation file tidy
  # filnam <- file.path(indir_netrad, paste0("ERA5Land_UTCDaily_netrad_",
  #                                          LON_string,".rds"))
  # df_net_radiation <- readr::read_rds(filnam)

  # read net radiation (shortwave 'ssr' and thermal 'str') file tidy
  df_ssr <- readr::read_rds(file.path(indir_ssr, paste0("ERA5Land_UTCDaily_tot_ssr_", LON_string,".rds")))
  df_str <- readr::read_rds(file.path(indir_str, paste0("ERA5Land_UTCDaily_tot_str_", LON_string,".rds")))

  # unnest all the data frames
  # df_net_radiation  <- df_net_radiation |> tidyr::unnest(data)
  df_ssr            <- df_ssr           |> tidyr::unnest(data)
  df_str            <- df_str           |> tidyr::unnest(data)
  df_patm           <- df_patm          |> tidyr::unnest(data)
  df_precip         <- df_precip        |> tidyr::unnest(data)
  df_tsurf          <- df_tsurf         |> tidyr::unnest(data)

  # unit conversions (and variable renaming)
  ## precipitation; total precip has units of m/day
  df_precip <-  df_precip |>
    mutate(precip = tot_tp * 1000 ) |> # conversion to mm/day
    dplyr::select(-tot_tp)

  # ## evaportranspiration; total PET has units of m/day
  # df_pev <-  df_pev |>
  #   mutate(pet = tot_pev * 1000 *-1) # conversion to mm/day and to positive values

  ## temperature
  df_tsurf  <-  df_tsurf  |>
    mutate(tsurf = mean_t2m - 273.15) |># conversion to °C
    dplyr::select(-mean_t2m)
  ## (CDO-precomputed) netrad is already in W/m^2;
  # df_net_radiation <- df_net_radiation

  ## shortwave and longwave surface radiation (ssr, str); have units of J/m2 per day => W/m2
  df_ssr <-  df_ssr |> mutate(ssr = tot_ssr/86400) # conversion to W/m2
  df_str <-  df_str |> mutate(str = tot_str/86400) # conversion to W/m2
  df_netrad <- dplyr::inner_join(df_ssr, df_str, by = join_by(lon, lat, datetime)) |>
    mutate(netrad = str + ssr) |>
    select(lon, lat, datetime, netrad, ssr, str) # NOTE: ssr and str are unused

  ## surface pressure; patm is already in Pa
  df_patm <-  df_patm |>
    mutate(patm = mean_sp)|> # conversion to mm/day and to positive values
    dplyr::select(-mean_sp)

  # data wrangling and time resolution adjustments
  df_precip <- df_precip |>
    mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
           year = lubridate::year(date),
           month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  df_tsurf  <- df_tsurf  |>
    mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
           year = lubridate::year(date),
           month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  df_patm <- df_patm |>
    mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
           year = lubridate::year(date),
           month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  # df_net_radiation <- df_net_radiation |>
  #   mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
  #          year = lubridate::year(date),
  #          month = lubridate::month(date)) |>
  #   dplyr::select(-datetime)

  df_netrad <- df_netrad |>
    mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
           year = lubridate::year(date),
           month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  # df_pev <- df_pev |>
  #   mutate(date = lubridate::ymd(datetime)) |>
  #   mutate(year = lubridate::year(date), month = lubridate::month(date)) |>
  #   dplyr::select(-datetime) |>
  #   group_by(year, month, lon, lat) |>
  #   summarise(pet = mean(pet, na.rm = TRUE), .groups = "drop") |>
  #   mutate(date = as.Date(paste0(year, "-", month, "-01")))

  ## merge all with daily data
  # pcwd
  df_pcwd <- df_precip |>  # one of the daily data frames
    # inner_join(df_net_radiation, by = c("lon", "lat", "year", "month", "date")) |>
    inner_join(df_netrad,       by = c("lon", "lat", "year", "month", "date")) |>
    inner_join(df_patm,          by = c("lon", "lat", "year", "month", "date")) |>
    inner_join(df_tsurf ,        by = c("lon", "lat", "year", "month", "date")) |>
    dplyr::select(year, month, date, lon, lat, precip, patm, tsurf, netrad) # netrad.x, netrad.y)

  # df_pcwd |> filter(abs(netrad.x - netrad.y) > 0.01) # this showed to be equivalent.

  # Ensure we had daily dat for all
  stopifnot(nrow(df_pcwd) == nrow(df_precip))
  stopifnot(nrow(df_pcwd) == nrow(df_netrad))
  # stopifnot(nrow(df_pcwd) == nrow(df_net_radiation))
  stopifnot(nrow(df_pcwd) == nrow(df_patm))
  stopifnot(nrow(df_pcwd) == nrow(df_tsurf ))


  # pet-calculation
  ## apply pet() function (instead of using pev from ERA5Land)
  df_pcwd <- df_pcwd |>
    mutate(pet = 60 * 60 * 24 * cwd::pet(netrad, tsurf, patm)) # conversion from mm s-1 to mm day-1

  # out pcwd
  out_pcwd <- df_pcwd |>
    dplyr::select(lon, lat, date, precip, tsurf, pet) |> # Use POTENTIAL ET as ET estimate

    # wrap time series for each grid cell (lon,lat combination) into a new column 'data'
    tidyr::nest(data = c(date, precip, tsurf, pet)) |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    ###slice(1:2)|> # uncomment for development/debugging   # 5secs for 1 pixel (75 years),
                                                            # 40secs for 10 pixel
                                                            # ==> then about 2h for 1800 pixel in 1 LON
                                                            # (in reality about 45 mins (due to ocean ?))
                                                            # (Note: this would amount to: 3600*0.75/24 = 112.5 days for single core).
                                                            # (      memory footprint was about 32GB per core)
    mutate(data = purrr::map(data, ~get_cwd_withSnow_and_reset_ERA5Land(.), .progress = TRUE))

  # write (complemented) data to cwd- and pcwd-files with meaningful name and index counter
  message(paste0("Writing file ", path_pcwd, " ..."))
  readr::write_rds(out_pcwd, path_pcwd)

  # don't return data - it's written to file
  return(NULL)
}


#LON_string <- "LON_-071.250"

ERA5Land_compute_pcwd_byLON <- function(
    LON_string,
    indir,
    outdir,
    reduce_rds_size = FALSE){ # NOTE: If reduce_rds_size then rds file content is reduced (and years are split depending on variant)
  tryCatch({
  #############################################
  # Define hardcoded paths and hardcoded options: change year and set number to adapt for other sets
  indir_prec      <- file.path(indir, "tot_tp")
  #indir_pev    <- file.path(indir, "total_pet") # unused, pet is computed with cwd::pet()
  indir_patm      <- file.path(indir, "mean_sp")
  indir_tas       <- file.path(indir, "mean_t2m")
  indir_str       <- file.path(indir, "tot_str")
  indir_ssr       <- file.path(indir, "tot_ssr")
  # prepare output names
  path_pcwd <- file.path(outdir, paste0("ERA5Land_pcwd", "_", LON_string, ".rds"))
  #############################################
  verbose_read_rds <- function(path) {
    tryCatch(
      readr::read_rds(path),
      error = function(e) {
        msg <- sprintf("Failed to read RDS file at '%s': %s",
                  normalizePath(path, mustWork = FALSE),
                  e$message)
        warning(msg)
        stop(msg,call. = FALSE)
      }
    )
  }
  #############################################


  print(paste0(Sys.time(), ", LON: ", LON_string))

  # read from files that contain tidy data for a single longitudinal band
  # read precipitation file tidy
  filnam <- file.path(indir_prec, paste0("ERA5Land_UTCDaily_tot_tp_",
                                         LON_string,".rds"))
  df_precip <- verbose_read_rds(filnam)

  # read temperature file tidy
  filnam <- file.path(indir_tas, paste0("ERA5Land_UTCDaily_mean_t2m_",
                                         LON_string,".rds"))
  df_tsurf  <- verbose_read_rds(filnam)

  # read surface Pressure file tidy
  filnam <- file.path(indir_patm, paste0("ERA5Land_UTCDaily_mean_sp_",
                                         LON_string,".rds"))
  df_patm <- verbose_read_rds(filnam)

  # read net radiation (shortwave 'ssr' and thermal 'str') file tidy
  df_ssr <- verbose_read_rds(file.path(indir_ssr, paste0("ERA5Land_UTCDaily_tot_ssr_", LON_string,".rds")))
  df_str <- verbose_read_rds(file.path(indir_str, paste0("ERA5Land_UTCDaily_tot_str_", LON_string,".rds")))

  # unnest all the data frames
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

  ## temperature
  df_tsurf  <-  df_tsurf  |>
    mutate(tsurf = mean_t2m - 273.15) |># conversion to °C
    dplyr::select(-mean_t2m)

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

  df_netrad <- df_netrad |>
    mutate(date = lubridate::ymd(sub("T.*", "", datetime)),
           year = lubridate::year(date),
           month = lubridate::month(date)) |>
    dplyr::select(-datetime)

  ## merge all with daily data
  # pcwd
  df_pcwd <- df_precip |>  # one of the daily data frames
    inner_join(df_netrad,       by = c("lon", "lat", "year", "month", "date")) |>
    inner_join(df_patm,          by = c("lon", "lat", "year", "month", "date")) |>
    inner_join(df_tsurf ,        by = c("lon", "lat", "year", "month", "date")) |>
    dplyr::select(year, month, date, lon, lat, precip, patm, tsurf, netrad) # netrad.x, netrad.y)

  # Ensure we had daily dat for all
  stopifnot(nrow(df_pcwd) == nrow(df_precip))
  stopifnot(nrow(df_pcwd) == nrow(df_netrad))
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
    mutate(data = purrr::map(data, ~get_cwd_withSnow_and_reset_ERA5Land(.), .progress = TRUE))

  # write (complemented) data to cwd- and pcwd-files with meaningful name and index counter
  message(paste0("Writing file(s) ", path_pcwd, " ..."))
  if (!reduce_rds_size) {
    readr::write_rds(out_pcwd, path_pcwd, compress = "xz")
  } else {
    out_pcwd |>
      # pcwd generated nested lists with elements 'inst' and 'df'. We only use df
      tidyr::unnest_wider(data) |> select(-inst) |> select(lon, lat, df) |>
      tidyr::unnest(df) |>
      select(lon, lat, date, pcwd_mm = deficit) |>
      dplyr::mutate(year = lubridate::year(date)) |>
      # variant 1:
      readr::write_rds(paste0(path_pcwd,"allyears_onlypcwd_mm.rds"), compress = "xz")
      # variant 2: additionally split by year
      # group_by(year) |> group_split() |>
      # purrr::map(function(df_peryear) {
      #   readr::write_rds(
      #     df_peryear,
      #     paste0(path_pcwd, "_", first(df_peryear$year), ".rds"),
      #     compress = "xz")
      #   })
  }

  # don't return data - it's written to file
  return(NULL)
  },error = function(e) {return(e$message)}) # error handling of tryCatch
}


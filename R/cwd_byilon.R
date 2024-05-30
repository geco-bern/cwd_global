cwd_byilon <- function(
    ilon,
    indir_evspsbl,
    indir_prec,
    indir_tas,
    indir_rlus,
    indir_rlds,
    indir_rsds,
    indir_rsus,
    indir_elevation,
    outdir_cwd,
    outdir_pcwd,
    fileprefix_cwd,
    fileprefix_pcwd
    ){

  # load functions that will be applied to time series
  source(paste0(here::here(), "/R/my_cwd.R"))
  source(paste0(here::here(), "/R/my_pcwd.R"))

  # read from file that contains tidy data for a single longitudinal band

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir_evspsbl,
    pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
    )
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- list.files(
    indir_prec,
    pattern = paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- list.files(
    indir_tas,
    pattern = paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- list.files(
    indir_rlus,
    pattern = paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlus <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rlds,
    pattern = paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsds,
    pattern = paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsus,
    pattern = paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsus <- readr::read_rds(filnam)

  # read elevation file tidy
  filnam <- list.files(
    indir_elevation,
    pattern = paste0("elevation_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_elevation <- readr::read_rds(filnam)

  # unnest all the data frames
  df_rsds <- df_rsds |> tidyr:unnest(data)
  df_rsus <- df_rsus |> tidyr:unnest(data)
  df_rlds <- df_rlds |> tidyr:unnest(data)
  df_rlus <- df_rlus |> tidyr:unnest(data)
  df_evap <- df_evap |> tidyr:unnest(data)
  df_prec <- df_prec |> tidyr:unnest(data)
  df_tas <- df_tas |> tidyr:unnest(data)
  df_elevation <- df_elevation |> tidyr:unnest(data)


  # resolution adjustments

  ## extract year and month from the time column
  df_prec <- df_prec |>
    mutate(time = as.Date(time))|>
    mutate(year = year(time), month = month(time))

  df_evap <- df_evap |>
    mutate(time = as.Date(time))|>
    mutate(year = year(time), month = month(time))

  ## cwd
  ### merge all such that monthly data is repeated for each day within month
  df_cwd <- df_prec |>  # one of the daily data frames
     left_join(df_evap, by = join_by(year, month))|>
     mutate(tas, df_tas$tas)

  df_cwd <- df_cwd |>
    select(-year, -month)

  ## compute net_radiation
  ### create new data frame
  df_radiation <- df_rsds |>
    mutate(rsus, df_rsus$rsus))|>
    mutate(rlds, df_rlds$rlds))|>
    mutate(rlus, df_rlus$rlus))|>

  ### calculate net radiation
  df_radiation <- df_radiation |>
    mutate(net_radiation = (rsds - rsus) + (rlds - rlus))

  ### selects only date and net radiation columns
  df_net_radiation <- df_radiation |>
    select(-rsds, -rsus, -rlds, -rlus)

  ### extract year and month from the time column
  df_net_radiation <- df_net_radiation |>
    mutate(time = as.Date(time))|>
    mutate(year = year(time), month = month(time))

  df_elevation <- df_elevation |>
    mutate(time = as.Date(time))|>
    mutate(year = year(time), month = month(time))

  ## pcwd
  ### merge all such that monthly data is repeated for each day within month
  df_pcwd <- df_prec |>  # one of the daily data frames
    left_join(df_net_radiation, by = join_by(year, month))|>
    left_join(df_elevation, by = join_by(year, month))|>
    mutate(tas, df_tas$tas)


  # out_cwd
  out_cwd <- df_cwd |>

    # group data by gridcells and wrap time series for each gridcell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    dplyr::mutate(data = purrr::map(data, ~my_cwd(.)))


  # out pcwd
  out_pcwd <- df_pcwd |>

    # group data by gridcells and wrap time series for each gridcell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    dplyr::mutate(data = purrr::map(data, ~my_pcwd(.)))


  # write (complemented) data to cwd-file with meaningful name and index counter
  path_cwd <- paste0(outdir_cwd, "/", fileprefix_cwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path, " ..."
    )
  )
  readr::write_rds(
    out_cwd,
    path_cwd
    )


  # write (complemented) data to pcwd-file.
  path_pcwd <- paste0(outdir_pcwd, "/", fileprefix_pcwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path, " ..."
    )
  )
  readr::write_rds(
    out_pcwd,
    path_pcwd
  )

  # don't return data - it's written to file
}

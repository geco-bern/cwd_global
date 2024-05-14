cwd_byilon <- function(
    ilon,
    indir,
    outdir,
    fileprefix
    ){

  # load function that will be applied to time series
  source(paste0(here::here(), "/R/my_cwd.R"))

  # read from file that contains tidy data for a single longitudinal band

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir,
    pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
    )
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- list.files(
    indir,
    pattern = paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- list.files(
    indir,
    pattern = paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- list.files(
    indir,
    pattern = paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlus <- readr::read_rds(filnam)

  filnam <- list.files(
    indir,
    pattern = paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir,
    pattern = paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir,
    pattern = paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsus <- readr::read_rds(filnam)


  # resolution adjustments
  # # merge all such that monthly data is repeated for each day within month
  # df <- df_prec |>  # one of the daily data frames
  #   tidyr::unnest(data) |>  # must unnest to join by date
  #   left_join(
  #     df_evap |>  # one of the monthly data frames
  #       tidyr::unnest(data),
  #     by = join_by(year, month)
  #   )

  # for demo only
  df <- df_evap |>
    tidyr::unnest(data)

  out <- df |>

    # Uncomment code below to nest data by gridcell, if not already nested.
    # group data by gridcells and wrap time series for each gridcell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    dplyr::mutate(data = purrr::map(data, ~my_cwd(.)))

  # write (complemented) data to file. Give it some meaningful name and the index counter
  path <- paste0(outdir, "/", fileprefix, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path, " ..."
    )
  )
  readr::write_rds(
    out,
    path
    )

  # don't return data - it's written to file
}

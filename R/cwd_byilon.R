cwd_byLON <- function(
    LON_string,
    indir,
    outdir,
    fileprefix,
    overwrite
    ){

  # prepare output
  # write (complemented) data to file. Give it some meaningful name and the index counter
  outpath <- file.path(outdir, paste0(fileprefix, "_", LON_string, ".rds"))
  if (file.exists(outpath) && !overwrite){

    # don't do anything
    return(paste0("File exists already: ", outpath))

  } else {
    # read from file that contains tidy data for a single longitudinal band

    # read evapotranspiration file tidy
    # filnam <- list.files(
    #   indir,
    #   pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_", LON_string, ".rds"), # NOTE REGEX not working since LON_string can contain a +
    #   full.names = TRUE,
    #   )
    filnam <- file.path(indir, paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_", LON_string, ".rds"))
    df_evap <- readr::read_rds(filnam)

    # read other required files (precipitation, temperature, ...

    # # merge all such that monthly data is repeated for each day within month
    # df <- df_prec |>  # one of the daily data frames
    #   tidyr::unnest(data) |>  # must unnest to join by date
    #   left_join(
    #     df_evap |>  # one of the monthly data frames
    #       tidyr::unnest(data),
    #     by = join_by(year, month)
    #   )

    # for demo only
    # df_evap <- df_evap |>
    #   tidyr::unnest(data)

    out <- df_evap |>
      # # Uncomment code below to nest data by gridcell, if not already nested.
      # # group data by gridcells and wrap time series for each gridcell into a new
      # # column, by default called 'data'.
      # dplyr::group_by(lon, lat) |>
      # tidyr::nest() |>

      # slice(1:2) |> # for demo only

      # apply the custom function on the time series data frame separately for
      # each gridcell.
      dplyr::mutate(data = purrr::map(data, ~my_cwd(.)))

    # write (complemented) data to file.
    message(
      paste0(
        "Writing file ", outpath, " ..."
      )
    )
    readr::write_rds(
      out,
      outpath
      )
    # don't return data - it's written to file
    return(paste0("Written results to: ", outpath))
  }
}

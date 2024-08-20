apply_fct_to_each_file <- function(
    fct_to_apply_per_location,
    filnam,
    outdir,
    overwrite,
    outfilename_template #"resultfile_[LONSTRING].rds"
){

  # prepare output
  stopifnot(grepl(pattern = "\\[LONSTRING\\]", outfilename_template))

  curr_LON_string <- gsub("^.*?(LON_[0-9.+-]*).rds$", "\\1", basename(filnam))
  outpath <- file.path(outdir, gsub("\\[LONSTRING\\]", curr_LON_string, outfilename_template))

  if (file.exists(outpath) && !overwrite){

    return(paste0("File exists already: ", outpath)) # don't do anything

  } else {
    # read from file that contains tidy data for a single longitudinal band

    # read evapotranspiration file tidy
    df_evap <- readr::read_rds(filnam)

    # apply the custom function on the time series data frame separately for each gridcell.
    out <- df_evap |>
      dplyr::mutate(data = purrr::map(data, ~fct_to_apply_per_location(.)))

    # write result to file
    message(paste0("Writing file ", outpath, " ..."))
    readr::write_rds(out, outpath)

    # don't return data - it's written to file
    return(paste0("Written results to: ", outpath))


    # UNUSED: stems originally from the function cwd_byLON()
    # read other required files (precipitation, temperature, ...
    # # merge all such that monthly data is repeated for each day within month
    # df <- df_prec |>  # one of the daily data frames
    #   tidyr::unnest(data) |>  # must unnest to join by date
    #   left_join(
    #     df_evap |>  # one of the monthly data frames
    #       tidyr::unnest(data),
    #     by = join_by(year, month)
    #   )
  }
}

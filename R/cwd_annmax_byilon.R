cwd_annmax_byLON <- function(
    filnam,
    outdir,
    overwrite
    ){

  # # write (complemented) data to file. Give it some meaningful name and the index counter
  outpath <- file.path(outdir, basename(filnam) %>% gsub(".rds","_ANNMAX.rds", .))
  if (file.exists(outpath) && !overwrite){

    # don't do anything
    return(paste0("File exists already: ", outpath))

  } else {

    # function to get annual maximum
    get_annmax <- function(df_of_one_coordinate){
      df_of_one_coordinate |>
        mutate(year = lubridate::year(datetime)) |>
        group_by(year) |>
        summarise(evspsbl_cum = max(evspsbl_cum))
    }

    # read evapotranspiration file tidy
    df <- readr::read_rds(filnam)

    # apply annual maximum function
    out <- df |>
      mutate(data = purrr::map(
        data,
        ~get_annmax(.)
      ))

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

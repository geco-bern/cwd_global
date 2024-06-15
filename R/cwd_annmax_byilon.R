cwd_annmax_byilon <- function(
    ilon,
    indir_cwd,
    indir_pcwd,
    outdir_cwd,
    outdir_pcwd,
    fileprefix_cwd,
    fileprefix_pcwd
    ){

  # function to get annual maximum
  get_annmax <- function(df){
    df |>
      mutate(year = lubridate::year(time)) |>
      group_by(year) |>
      summarise(evspsbl_cum = max(evspsbl_cum))
  }

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir,
    pattern = paste0(fileprefix, "_", ilon, ".rds"),
    full.names = TRUE
    )
  df <- readr::read_rds(filnam)

  # apply annual maximum function
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~get_annmax(.)
    ))

  # write (complemented) data to file. Give it some meaningful name and the index counter
  path <- paste0(outdir, "/", fileprefix, "_", ilon, "_ANNMAX.rds")
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

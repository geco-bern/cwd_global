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
      summarise(max_deficit = max(deficit))
  }


  # read cwd file tidy
  filnam <- list.files(
    indir_cwd,
    pattern = paste0(fileprefix_cwd, "_", ilon, ".rds"),
    full.names = TRUE
  )
  df_cwd <- readr::read_rds(filnam)


  # read pcwd file tidy
  filnam <- list.files(
    indir_pcwd,
    pattern = paste0(fileprefix_pcwd, "_", ilon, ".rds"),
    full.names = TRUE
  )
  df_pcwd <- readr::read_rds(filnam)


  # apply annual maximum function
  out_cwd <- df_cwd |>
    mutate(data = purrr::map(
      data,
      ~get_annmax(.)
    ))

  out_pcwd <- df_pcwd |>
    mutate(data = purrr::map(
      data,
      ~get_annmax(.)
    ))

  # write (complemented) data to file. Give it some meaningful name and the index counter
  ##cwd
  path <- paste0(outdir_cwd, "/", fileprefix_cwd, "_", ilon, "_ANNMAX.rds")
  message(
    paste0(
      "Writing file ", path, " ..."
    )
  )
  readr::write_rds(
    out_cwd,
    path
    )

  ## pcwd
  path <- paste0(outdir_pcwd, "/", fileprefix_pcwd, "_", ilon, "_ANNMAX.rds")
  message(
    paste0(
      "Writing file ", path, " ..."
    )
  )
  readr::write_rds(
    out_pcwd,
    path
  )

  # don't return data - it's written to file
}

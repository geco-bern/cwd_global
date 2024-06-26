collect_cwd_annmax_byilon <- function(
    ilon,
    indir,
    fileprefix,
){

  # read annual time series file
  filnam <- list.files(
    indir,
    pattern = paste0(fileprefix, "_", ilon, "_ANNMAX.rds"),
    full.names = TRUE
  )
  df <- readr::read_rds(filnam) |>
    unnest(data)


  return(df)
}

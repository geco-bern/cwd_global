collect_cwd_annmax_byilon <- function(
    ilon,
    indir_cwd,
    indir_pcwd,
    fileprefix_cwd,
    fileprefix_pcwd,
){

  # read annual cwd time series file
  filnam <- list.files(
    indir_cwd,
    pattern = paste0(fileprefix_cwd, "_", ilon, "_ANNMAX.rds"),
    full.names = TRUE
  )
  df_cwd <- readr::read_rds(filnam) |>
    unnest(data)

  # read annual pcwd time series file
  filnam <- list.files(
    indir_pcwd,
    pattern = paste0(fileprefix_pcwd, "_", ilon, "_ANNMAX.rds"),
    full.names = TRUE
  )
  df_pcwd <- readr::read_rds(filnam) |>
    unnest(data)




  return(df)
}

# function to get annual maximum
get_annmax <- function(df){
  df |>
    mutate(year = lubridate::year(time)) |>
    group_by(year) |>
    summarise(max_deficit = max(deficit))
}
get_cwd_annmax_byLON <- function(
    in_fname,
    outdir
    ){

  # read cwd file tidy
  df <- readr::read_rds(in_fname)

  # apply annual maximum function
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~get_annmax(.)
    ))
  # test: out |> slice(1) |> unnest(data) |> print(n=100)

  # write (reduced) data to cwdfile with meaningful name and index counter
  path <- file.path(outdir, gsub('.rds', '_ANNMAX.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))
  readr::write_rds(out, path)

  # don't return data - it's written to file
  return(NULL)
}

# Function to get annual maximum deficit and corresponding DOY
get_annmax_doy <- function(df) {
  df |>
    mutate(year = lubridate::year(date)) |>
    group_by(year) |>
    slice_max(deficit, n = 1, with_ties = FALSE) |>  # Select row with max deficit
    summarise(
      doy_max_deficit = first(doy)  # Extract DOY where max deficit occurred
    )
}

get_cwd_annmax_doy_byLON <- function(
    in_fname,
    outdir
){

  # read cwd file tidy
  df <- readr::read_rds(in_fname)

  # apply annual maximum function
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~get_annmax_doy(.x$df)
    ))
  # test: out |> slice(1) |> unnest(data) |> print(n=100)

  # write (reduced) data to cwdfile with meaningful name and index counter
  path <- file.path(outdir, gsub('.rds', '_ANNMAX_DOY.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))
  readr::write_rds(out, path)

  # don't return data - it's written to file
  return(NULL)
}


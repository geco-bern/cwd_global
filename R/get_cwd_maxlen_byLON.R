# function to get daily deficit data
get_maxlen <- function(df){
    df |>
    mutate(year = lubridate::year(date_start)) |>
    group_by(year) |>
    summarise(max_len = max(len, na.rm = TRUE))  # Select only the relevant columns
}

get_cwd_maxlen_byLON <- function(
    in_fname,
    outdir
){

  # read cwd file tidy
  df <- readr::read_rds(in_fname)

  # apply annual maximum function
  out <- df |>
    mutate(data = purrr::map(
      data,
      ~get_maxlen(.x$inst) #adapt this
    ))
  # test: out |> slice(1) |> unnest(data) |> print(n=100)

  # write (reduced) data to cwdfile with meaningful name and index counter
  path <- file.path(outdir, gsub('.rds', '_MAXLEN.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))
  readr::write_rds(out, path)

  # don't return data - it's written to file
  return(NULL)
}


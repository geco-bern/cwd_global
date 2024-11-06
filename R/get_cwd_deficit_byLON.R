# function to get daily deficit data

get_deficit <- function(df) {
  df |>
    select(date, deficit, precip, pet) |>   # Select only the relevant columns
    filter(!is.na(deficit))     # Optionally filter out NA values if needed
}

get_inst <- function(df) {
  df |>
    select(len, date_start, date_end, deficit) |>   # Select only the relevant columns
    filter(!is.na(deficit))     # Optionally filter out NA values if needed
}

#function to apply to dataframe
get_cwd_deficit_byLON <- function(
    in_fname,
    outdir
){

  # read cwd file tidy
  df <- readr::read_rds(in_fname)

  # apply annual maximum function
  out_def <- df |>
    mutate(data = purrr::map(
      data,
      ~get_deficit(.x$df) #adapt this
    ))
  # test: out |> slice(1) |> unnest(data) |> print(n=100)

  # write (reduced) data to cwdfile with meaningful name and index counter
  path <- file.path(outdir, gsub('.rds', '_DEFICIT.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))
  readr::write_rds(out_def, path)

  # don't return data - it's written to file
  return(NULL)
}

get_cwd_instance_byLON <- function(
    in_fname,
    outdir
){

  # read cwd file tidy
  df <- readr::read_rds(in_fname)

  # apply annual maximum function
  out_inst <- df |>
    mutate(data = purrr::map(
      data,
      ~get_inst(.x$inst) #adapt this
    ))
  # test: out |> slice(1) |> unnest(data) |> print(n=100)

  # write (reduced) data to cwdfile with meaningful name and index counter
  path <- file.path(outdir, gsub('.rds', '_INST.rds', basename(in_fname)))
  message(paste0("Writing file ", path, " ..."))
  readr::write_rds(out_inst, path)

  # don't return data - it's written to file
  return(NULL)
}

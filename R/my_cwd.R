my_cwd <- function(df){

  # arguments:
  # df: a data frame. must contain certain columns with specific names used here

  # just an example!
  out <- df |>
    mutate(evspsbl_cum = evspsbl) |>

    # reduce size - important
    select(time, evspsbl_cum)

  # return a data frame
  return(out)
}

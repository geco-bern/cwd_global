my_cwd <- function(df){

  # loading libraries
  library(tidyr)
  library(cwd)
  library(rpmodel)
  library(dplyr)


  # nested dataframe is called `df` with the column with the list of variables called `data`
  vars_df <- unnest(df, data)


  # unit conversions
  ## evapotranspiration
  vars_df$evap <- vars_df$evap * 86400 # conversion to mm day-1

  ## precipitation
  vars_df$prec <- vars_df$prec * 86400 # conversion to mm day-1

  ## temperature
  vars_df$tas <- vars_df$tas - 273.15 # conversion to °C


  # snow simulation
  vars_df <- vars_df |>
    mutate(precipitation = ifelse(tas < 0, 0, prec),
           snow = ifelse(tas < 0, prec, 0)) |>
    simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_tas = "tas")


  vars_df <- vars_df |>
    mutate(wbal = liquid_to_soil - evap)


  # cwd
  ## calculate cumulative water deficit
  out_cwd <- cwd(vars_df,
                 varname_wbal = "wbal",
                 varname_date = "time",
                 thresh_terminate = 0.0,
                 thresh_drop = 0.0)

  out_cwd$inst <- out_cwd$inst |>
    filter(len >= 20)

  out_cwd <- out_cwd |>
    select(lon, lat, time, deficit)

  # return a data frame
  return(out_cwd)
}

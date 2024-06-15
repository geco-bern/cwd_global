#' @export


my_cwd <- function(data){

  # loading libraries
  library(tidyr)
  library(cwd)
  library(rpmodel)
  library(dplyr)

  # for testing read in rds tibble
  # test_tibble <- readRDS(paste0(here::here(), "/data/test_tibble.rds"))


  # convert tibble to dataframe
  vars_df <- as.data.frame(data)


  # unit conversions
  ## evapotranspiration
  vars_df$evspsbl <- vars_df$evspsbl * 86400 # conversion to mm day-1

  ## precipitation
  vars_df$pr <- vars_df$pr * 86400 # conversion to mm day-1

  ## temperature
  vars_df$tas <- vars_df$tas - 273.15 # conversion to °C


  # snow simulation
  vars_df <- vars_df |>
    mutate(precipitation = ifelse(tas < 0, 0, pr),
           snow = ifelse(tas < 0, pr, 0)) |>
    simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tas")


  vars_df <- vars_df |>
    mutate(wbal = liquid_to_soil - evspsbl)


  # cwd
  ## calculate cumulative water deficit
  out_cwd <- cwd(vars_df,
                 varname_wbal = "wbal",
                 varname_date = "time",
                 thresh_terminate = 0.0,
                 thresh_drop = 0.0)

  out_cwd$inst <- out_cwd$inst |>
    filter(len >= 20)

  out_cwd$df <- out_cwd$df |>
    select(time, deficit)


  # return data frame
  return(out_cwd$df)
}

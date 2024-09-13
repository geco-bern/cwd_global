get_cwd_withSnow_and_reset <- function(vars_df){
  # vars_df must contain columns: time, pr, evspsbl, tas

  # loading libraries
  require(dplyr)
  require(tidyr)
  require(cwd)
  library(rpmodel)    # Need to reload library on each computation core. When using require() instead of library() inside of `multiplyr` get error message:
                      #   Error in .External(list(name = "CppMethod__invoke_notvoid", address = <pointer: (nil)>,  :
                      #   NULL value passed as symbol address

  # cwd reset
  ## average monthly P-ET over the first 30 years of the time series
  reset_df <- vars_df |>
    mutate(year = lubridate::year(time)) |>
    mutate(month = lubridate::month(time))|>
    mutate(pr_et = pr-evspsbl)|>
    filter(year < 2045)|>
    group_by(month) |>
    summarize(mean_pr_et = mean(pr_et))

  ## which month P-ET maximal
  max_index <- which.max(reset_df$mean_pr_et)
  max_month <- reset_df$month[max_index]

  ## day_of_year as param doy_reset in cwd-algorithm
  ## corresponds to day-of-year (integer) when deficit is to be reset to zero
  date_str <- paste0("2015-", max_month, "-01")
  date_obj <- as.Date(date_str, format = "%Y-%m-%d")
  day_of_year <- lubridate::yday(date_obj)


  # snow simulation
  vars_df <- vars_df |>
    mutate(precipitation = ifelse(tas < 0, 0, pr),
           snow = ifelse(tas < 0, pr, 0)) |>
    cwd::simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tas")


  vars_df <- vars_df |>
    mutate(wbal = liquid_to_soil - evspsbl)


  # cwd
  ## calculate cumulative water deficit
  out_cwd <- cwd::cwd(vars_df,
                 varname_wbal = "wbal",
                 varname_date = "time",
                 thresh_terminate = 0.0,
                 thresh_drop = 0.0,
                 doy_reset= day_of_year)

  out_cwd$inst <- out_cwd$inst |>
    filter(len >= 20)

  out_cwd$df <- out_cwd$df |>
    select(time, deficit)


  # return data frame
  return(out_cwd$df)
}

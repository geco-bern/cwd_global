get_cwd_withSnow_and_reset_ModESim <- function(vars_df){
#TODO: have as function again
#vars_df <- df_pcwd
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
    mutate(date = lubridate::ymd(date)) |>
    mutate(month = lubridate::month(date))|>
    mutate(pr_et = precip - pet)|> #replace with et for CWD
    group_by(month) |>
    summarize(mean_pr_et = mean(pr_et))

  ## which month P-ET maximal
  max_index <- which.max(reset_df$mean_pr_et)
  max_month <- reset_df$month[max_index]

  ## day_of_year as param doy_reset in cwd-algorithm
  ## corresponds to day-of-year (integer) when deficit is to be reset to zero
  date_str <- paste0("1850-", max_month, "-01") #can be any random year
  date_obj <- as.Date(date_str, format = "%Y-%m-%d")
  day_of_year <- lubridate::yday(date_obj)


  # snow simulation
  vars_df <- vars_df |>
    mutate(precipitation = ifelse(tsurf < 0, 0, precip),
           snow = ifelse(tsurf < 0, precip, 0)) |>
    cwd::simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tsurf")


  vars_df <- vars_df |>
    mutate(wbal = liquid_to_soil - pet)


  # cwd
  ## calculate cumulative water deficit
  out_cwd <- cwd::cwd(vars_df,
                      varname_wbal = "wbal",
                      varname_date = "date",
                      thresh_terminate = 0.0,
                      thresh_drop = 0.0,
                      doy_reset= day_of_year)

  # return list with two components:
  # - data frame for time series and all variables
  # - data frame with instances
   return(out_cwd)
 }

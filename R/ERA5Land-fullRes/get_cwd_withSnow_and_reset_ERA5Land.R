get_cwd_withSnow_and_reset_ERA5Land <- function(vars_df){
  # vars_df must contain columns: time, pr, evspsbl, tas

  # cwd reset
  ## average monthly P-ET over the whole time series
  reset_df <- vars_df |>
    mutate(date = lubridate::ymd(date)) |>
    mutate(month = lubridate::month(date))|>
    mutate(pr_et = precip - pet)|> #replace with et for CWD
    group_by(month) |>
    summarize(mean_pr_et = mean(pr_et, na.rm = TRUE))

  ## which month P-ET maximal
  max_index <- which.max(reset_df$mean_pr_et)
  max_month <- reset_df$month[max_index]

  ## day_of_year as param doy_reset in cwd-algorithm
  ## corresponds to day-of-year (integer) when deficit is to be reset to zero
  date_str <- paste0("1950-", max_month, "-01") #can be any random year
  date_obj <- as.Date(date_str, format = "%Y-%m-%d")
  day_of_year <- lubridate::yday(date_obj)

  if (all(is.na(vars_df$precip))) {     # ocean tile from ERA5Land: return NaN (SOLUTION A)
    out_cwd <- list(
      inst = tibble(idx_start = NA_real_, len = NA_real_, iinst = NA_real_,
                    date_start = as.Date(c()), date_end = as.Date(c()),
                    deficit = NA_real_),
      df = vars_df |> mutate(doy = NA_real_, iinst = NA_real_, dday = NA_real_, deficit = NA_real_)
    )
  } else {                              # land tile from ERA5Land: return cwd
    # snow simulation --- require tsurf for ERA5Land
    vars_df <- vars_df |>
      mutate(precipitation = ifelse(tsurf < 0, 0, precip),
             snow          = ifelse(tsurf < 0, precip, 0)) |>
      cwd::simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tsurf")

    vars_df <- vars_df |>
      mutate(wbal = liquid_to_soil - pet)

    # cwd
    ## calculate cumulative water deficit
    out_cwd <- cwd::cwd(vars_df,
                        varname_wbal = "wbal",
                        varname_date = "date",
                        ##choose between absolute or relative cwd threshold or doy_reset:
                        #thresh_terminate = 0.0,
                        #thresh_terminate_absolute = NA,
                        thresh_drop = 0.0,    # set to 0 as we do not want remove days after deficit release
                        doy_reset= day_of_year
    )
  }

  # return list with two components:
  # - data frame for time series and all variables
  # - data frame with instances
  return(out_cwd)
  # browser()
  # ggplot(out_cwd$df |> filter(date > "2020-01-01"), aes(x=date,y=deficit)) + geom_line()
}

my_cwd <- function(df_prec, df_evap, df_tas){

  # loading libraries
  library(terra)
  library(readr)
  library(tidyr)
  library(ggplot2)
  library(viridis)
  library(viridisLite)
  library(weathermetrics)
  library(ncdf4)
  library(chron)
  library(RColorBrewer)
  library(lattice)
  library(cwd)
  library(lubridate)
  library(rpmodel)

  # column renaming and unit conversions

  # evapotranspiration
  colnames(df_evap) <- c("date", "evapotranspiration") # column renaming
  df_evap$evapotranspiration <- df_evap$evapotranspiration * 86400 # conversion to mm day-1

  # precipitation
  colnames(df_prec) <- c("date", "precipitation") # column renaming
  df_prec$precipitation <- df_prec$precipitation * 86400 # conversion to mm day-1

  # temperature
  colnames(df_tas) <- c("date", "temperature") # column renaming
  df_tas$temperature <- df_tas$temperature - 273.15 # conversion to °C

  # time-range adjustments


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

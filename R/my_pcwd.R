# my pcwd

my_pcwd <- function(df_prec, df_evap, df_tas, df_rlus, df_rlds, df_rsds, df_rsus){

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
  library(bigleaf)

  # column renaming and unit conversions

  ## evapotranspiration
  colnames(df_evap) <- c("date", "evapotranspiration") # column renaming
  df_evap$evapotranspiration <- df_evap$evapotranspiration * 86400 # conversion to mm day-1

  ## precipitation
  colnames(df_prec) <- c("date", "precipitation") # column renaming
  df_prec$precipitation <- df_prec$precipitation * 86400 # conversion to mm day-1

  ## temperature
  colnames(df_tas) <- c("date", "temperature") # column renaming
  df_tas$temperature <- df_tas$temperature - 273.15 # conversion to °C

  ## radiation
  colnames(df_rlus) <- c("date", "up_longwave_radiation") # column renaming
  colnames(df_rlds) <- c("date", "down_longwave_radiation") # column renaming
  colnames(df_rsds) <- c("date", "down_shortwave_radiation") # column renaming
  colnames(df_rsus) <- c("date", "up_shortwave_radiation") # column renaming

  ### merge radiation variables into one dataframe
  radiation_df <- merge(merge(merge(df_rlus, df_rlds, by = "date"), df_rsds, by = "date"), df_rsus, by = "date")

  ### calculate net radiation and add to dataframe
  radiation_df$net_radiation <- (radiation_df$down_shortwave_radiation - radiation_df$up_shortwave_radiation) + (radiation_df$down_longwave_radiation- radiation_df$up_longwave_radiation)

  net_radiation_df <- select(radiation_df, date, net_radiation)


  # time-range adjustments

  ## create vars_df


  # PET Calculation

  ## calculate equilibrium evapotranspiration (eet) using the equilibrium.ET function
  vars_df$eet <- equilibrium.ET(
    Tair = vars_df$temperature,
    Rn = vars_df$rnet
  )

  ## convert from energy units (W/m²) to mass units (mm/day)
  ### 1 W/m² is approximately equal to 0.0354 mm/day (latent heat flux conversion)
  eet_mm_day <- eet * 0.0354

  ## remove energy units row from dataframe
  vars_df <- select(vars_df, -rnet)

  ## calculate factor 1.26 * eet
  vars_df$potential_evapotranspiration <- vars_df$eet * 1.26


}

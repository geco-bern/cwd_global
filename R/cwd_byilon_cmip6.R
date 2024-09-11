#' @export

cwd_byilon <- function(
    ilon,
    indir_evspsbl,
    indir_prec,
    indir_tas,
    indir_rlus,
    indir_rlds,
    indir_rsds,
    indir_rsus,
    indir_elevation,
    outdir_cwd,
    outdir_pcwd,
    fileprefix_cwd,
    fileprefix_pcwd
    ){


  # load functions that will be applied to time series
  #' @importFrom here here
  source(paste0(here::here(), "/R/my_cwd.R"))
  source(paste0(here::here(), "/R/my_pcwd.R"))


  # load necessary libraries
  library(tidyr)
  library(cwd)
  library(rpmodel)
  library(dplyr)


  # read from file that contains tidy data for a single longitudinal band
  #' @importFrom readr read_rds

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir_evspsbl,
    pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
    )
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- list.files(
    indir_prec,
    pattern = paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- list.files(
    indir_tas,
    pattern = paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- list.files(
    indir_rlus,
    pattern = paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlus <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rlds,
    pattern = paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rlds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsds,
    pattern = paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsus,
    pattern = paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", ilon, ".rds"),
    full.names = TRUE
  )
  df_rsus <- readr::read_rds(filnam)


  # read elevation file and convert to data frame
  library(terra)
  filnam <- list.files(
    indir_elevation,
    pattern = paste0("elevation.nc"),
    full.names = TRUE
  )

  rasta_elevation <- terra::rast(filnam)

  ## read the needed longitude value and extract the latitude values
  lon <- df_prec[["lon"]][1]
  latitudes <- unique(df_prec$lat)

  ## create a data frame of the coordinates
  loc <- data.frame(lon = rep(lon, length(latitudes)), lat = latitudes)

  ## convert to vector points
  points <- terra::vect(loc, geom = c("lon", "lat"), crs = "EPSG:4326")

  ## extract values
  vals <- terra::extract(rasta_elevation, points, xy = FALSE, ID = FALSE, method = "simple")

  # combine coordinates with extracted values
  df_elevation <- data.frame(
    lon = lon,
    lat = latitudes,
    value = vals
  )

  ## set NA-value at 90 degrees to 0
  df_elevation[is.na(df_elevation)] <- 0


  # unnest all the data frames
  #' @importFrom tidyr unnest
  df_rsds <- df_rsds |> tidyr::unnest(data)
  df_rsus <- df_rsus |> tidyr::unnest(data)
  df_rlds <- df_rlds |> tidyr::unnest(data)
  df_rlus <- df_rlus |> tidyr::unnest(data)
  df_evap <- df_evap |> tidyr::unnest(data)
  df_prec <- df_prec |> tidyr::unnest(data) # lon lat pr time
  df_tas <- df_tas |> tidyr::unnest(data)


  # data wrangling and resolution adjustments

  ## extract year and month from the time column
  df_prec <- df_prec |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))

  df_evap <- df_evap |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))|>
    select(-time)

  ## cwd
  ### merge all such that monthly data is repeated for each day within month
  df_cwd <- df_prec |>
    left_join(df_evap, by = join_by(lon, lat, year, month)) |>
    left_join(df_tas, by = join_by(lon, lat, time))|>
    dplyr::select(-year, -month)

  ### convert tibble to dataframe
  df_cwd <- as.data.frame(df_cwd)

  ### unit conversions
  #### evapotranspiration
  df_cwd$evspsbl <- df_cwd$evspsbl * 86400 # conversion to mm day-1

  #### precipitation
  df_cwd$pr <- df_cwd$pr * 86400 # conversion to mm day-1

  #### temperature
  df_cwd$tas <- df_cwd$tas - 273.15 # conversion to °C

  ### cwd reset
  #### average monthly P-ET over the first 30 years of the time series
  reset_df <- df_cwd |>
    mutate(year = lubridate::year(time)) |>
    mutate(month = lubridate::month(time))|>
    mutate(pr_et = pr-evspsbl)|>
    filter(year < 2045)|>
    group_by(month) |>
    summarize(mean_pr_et = mean(pr_et))

  #### which month P-ET maximal
  max_index <- which.max(reset_df$mean_pr_et)
  max_month <- reset_df$month[max_index]

  #### day_of_year as param doy_reset in cwd-algorithm
  #### corresponds to day-of-year (integer) when deficit is to be reset to zero
  date_str <- paste0("2015-", max_month, "-01")
  date_obj <- as.Date(date_str, format = "%Y-%m-%d")
  day_of_year <- lubridate::yday(date_obj)

  ### snow simulation
  df_cwd <- df_cwd |>
    mutate(precipitation = ifelse(tas < 0, 0, pr),
           snow = ifelse(tas < 0, pr, 0)) |>
    cwd::simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tas")


  df_cwd <- df_cwd |>
    mutate(wbal = liquid_to_soil - evspsbl)


  # out_cwd
  out_cwd <- df_cwd |>

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    #' @importFrom tidyr nest
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    #' @importFrom purrr map
    mutate(data = purrr::map(data, ~cwd::cwd(.,
                                           varname_wbal = "wbal",
                                           varname_date = "time",
                                           thresh_terminate = 0.0,
                                           thresh_drop = 0.0,
                                           doy_reset= day_of_year)))


  out_cwd$inst <- out_cwd$inst |>
    filter(len >= 20)

  out_cwd$df <- out_cwd$df |>
    select(time, deficit)


  ## compute net_radiation
  ### create new data frame
  df_radiation <- df_rsds |>
    left_join(df_rsus, by = join_by(lon, lat, time))|>
    left_join(df_rlds, by = join_by(lon, lat, time))|>
    left_join(df_rlus, by = join_by(lon, lat, time))

  ### calculate net radiation
  df_net_radiation <- df_radiation |>
    mutate(net_radiation = (rsds - rsus) + (rlds - rlus))|>
    select(-rsds, -rsus, -rlds, -rlus)

  ### extract year and month from the time column
  df_net_radiation <- df_net_radiation |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))|>
    select(-time)

  ## pcwd
  ### merge all such that monthly data is repeated for each day within month
  df_pcwd <- df_prec |>  # one of the daily data frames
    left_join(df_net_radiation, by = join_by(lon, lat, year, month))|>
    left_join(df_tas, by = join_by(lon, lat, time))|>
    left_join(df_elevation, by = join_by(lon, lat))|>
    dplyr::select(-year, -month)

  ### convert tibble to dataframe
  df_pcwd <- as.data.frame(df_pcwd)

  ### unit conversions
  #### precipitation
  df_pcwd$pr <- df_pcwd$pr * 86400 # conversion to mm day-1

  #### temperature
  df_pcwd$tas <- df_pcwd$tas - 273.15 # conversion to °C

  ### pet-calculation
  #### calculate surface pressure
  source(paste0(here::here(), "/R/calc_patm.R"))
  df_pcwd$patm <- calc_patm(df_pcwd$elevation)

  #### apply pet() function
  df_pcwd <- df_pcwd |>
    mutate(pet = 60 * 60 * 24 * pet(net_radiation, tas, patm))

  ### pcwd reset
  #### average monthly pr-pet over the first 30 years of the time series
  reset_df <- df_pcwd |>
    mutate(year = lubridate::year(time)) |>
    mutate(month = lubridate::month(time))|>
    mutate(pr_pet = pr-pet)|>
    filter(year < 2045)|>
    group_by(month) |>
    summarize(mean_pr_pet = mean(pr_pet))


  ## which month pr-pet maximal
  max_index <- which.max(reset_df$mean_pr_pet)
  max_month <- reset_df$month[max_index]


  ## day_of_year as param doy_reset in cwd-algorithm
  ## corresponds to day-of-year (integer) when deficit is to be reset to zero
  date_str <- paste0("2015-", max_month, "-01")
  date_obj <- as.Date(date_str, format = "%Y-%m-%d")
  day_of_year <- lubridate::yday(date_obj)

  ### snow simulation
  df_pcwd <- df_pcwd |>
    mutate(precipitation = ifelse(tas < 0, 0, pr),
           snow = ifelse(tas < 0, pr, 0)) |>
    simulate_snow(varnam_prec = "precipitation", varnam_snow = "snow", varnam_temp = "tas")


  df_pcwd <- df_pcwd |>
    mutate(wbal_pet = liquid_to_soil - pet)


  # out pcwd
  out_pcwd <- df_pcwd |>

    # group data by grid cells and wrap time series for each grid cell into a new
    # column, by default called 'data'.
    group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each grid cell.
    mutate(data = purrr::map(data, ~cwd::cwd(.,
                                           varname_wbal = "wbal_pet",
                                           varname_date = "time",
                                           thresh_terminate = 0.0,
                                           thresh_drop = 0.0,
                                           doy_reset= day_of_year)))


  out_pcwd$inst <- out_pcwd$inst |>
    filter(len >= 20)

  out_pcwd$df <- out_pcwd$df |>
    select(time, deficit)


  # write (complemented) data to cwd-file with meaningful name and index counter
  path_cwd <- paste0(outdir_cwd, "/", fileprefix_cwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path_cwd , " ..."
    )
  )
  #' @importFrom readr write_rds
  readr::write_rds(
    out_cwd$df,
    path_cwd
    )


  # write (complemented) data to pcwd-file.
  path_pcwd <- paste0(outdir_pcwd, "/", fileprefix_pcwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path_pcwd, " ..."
    )
  )
  readr::write_rds(
    out_pcwd$df,
    path_pcwd
  )

  # don't return data - it's written to file
}

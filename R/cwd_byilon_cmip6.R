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


  # read from file that contains tidy data for a single longitudinal band
  #' @importFrom readr read_rds

  # read evapotranspiration file tidy
  filnam <- list.files(
    indir_evspsbl,
    pattern = paste0("evspsbl_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
    )
  df_evap <- readr::read_rds(filnam)


  # read precipitation file tidy
  filnam <- list.files(
    indir_prec,
    pattern = paste0("pr_day_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_prec <- readr::read_rds(filnam)


  # read temperature file tidy
  filnam <- list.files(
    indir_tas,
    pattern = paste0("tas_day_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_tas <- readr::read_rds(filnam)


  # read radiation files tidy
  filnam <- list.files(
    indir_rlus,
    pattern = paste0("rlus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_rlus <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rlds,
    pattern = paste0("rlds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_rlds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsds,
    pattern = paste0("rsds_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_rsds <- readr::read_rds(filnam)

  filnam <- list.files(
    indir_rsus,
    pattern = paste0("rsus_mon_CESM2_ssp585_r1i1p1f1_native_ilon_", 9, ".rds"),
    full.names = TRUE
  )
  df_rsus <- readr::read_rds(filnam)

  # read elevation file and convert to dataframe
  filnam <- list.files(
    indir_elevation,
    pattern = paste0("elevation.nc"),
    full.names = TRUE
  )
  #' @import ncdf4
  library(ncdf4)
  df_elevation <- ncdf4::nc_open(filnam)

  ## extract variables
  lon <- ncvar_get(df_elevation, "easting")
  lat <- ncvar_get(df_elevation, "northing")
  elevation <- ncvar_get(df_elevation, "elevation")

  ## create a grid of longitude and latitude values
  lon_lat_grid <- expand.grid(lon = lon, lat = lat)

  # convert the elevation matrix into a vector
  ## matrix(latitude,longitude)
  elev_vector <- as.vector(elevation)

  # Combine the grid and elevation data into a dataframe
  df_elevation <- cbind(lon_lat_grid, elevation = elev_vector)
  ## replace na-values with 0
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

  ## extract year and month from the time column
  df_net_radiation <- df_net_radiation |>
    mutate(year = lubridate::year(time), month = lubridate::month(time))|>
    select(-time)

  ## extract current longitude value from elevation
  ### sort the dataframe by longitude
  df_sorted <- df_elevation |>
    arrange(lon)

  ### create same indices for same values
  df_sorted <- df_sorted |>
    mutate(index = as.integer(factor(lon, levels = unique(lon))))

  ### extract values that match current ilon
  ilon <- 9 # for testing
  matching_values <- df_sorted[df_sorted$index == ilon, ]

  ### reverse the order of latitude values
  matching_values_sorted <- matching_values |>
    arrange(lat) |>
    select(-index, -lon, -lat) |>
    mutate(index = row_number())

  ### create index column to join with elevation data
  df_prec_index <- as.data.frame(df_prec) |>
    mutate(index = as.integer(factor(lat, levels = unique(lat))))


  ## pcwd
  ### merge all such that monthly data is repeated for each day within month
  df_pcwd <- df_prec_index |>  # one of the daily data frames
    left_join(df_net_radiation, by = join_by(lon, lat, year, month))|>
    left_join(df_tas, by = join_by(lon, lat, time))|>
    left_join(matching_values_sorted, by = join_by(index))|>
    dplyr::select(-year, -month, -index)

  df_pcwd <- as_tibble(df_pcwd)


  # out_cwd
  out_cwd <- df_cwd |>

    # group data by gridcells and wrap time series for each gridcell into a new
    # column, by default called 'data'.
    dplyr::group_by(lon, lat) |>
    #' @importFrom tidyr nest
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    #' @importFrom purrr map
    mutate(data = purrr::map(data, ~my_cwd(.)))


  # for testing
  # tibble <- out_cwd %>%
  #  pull(data) %>%
  #  .[[1]]
  # saveRDS(tibble, paste0(here::here(), "/data-raw/vars_tibble.rds"))

  # for comparison with fluxnet data
  ## extract nearest latitude and write to rds files
  start <- 51
  end <- 52
  extracted_cwd <- df_cwd[(df_cwd$lat >= start) & (df_cwd$lat <= end), ]
  extracted_pcwd <- df_pcwd[(df_pcwd$lat >= start) & (df_pcwd$lat <= end), ]
  saveRDS(extracted_cwd, paste0(here::here(), "/data-raw/extracted_vars_DE-Hai_cwd.rds"))
  saveRDS(extracted_pcwd, paste0(here::here(), "/data-raw/extracted_vars_DE-Hai_pcwd.rds"))


  # out pcwd
  out_pcwd <- df_pcwd |>

    # group data by gridcells and wrap time series for each gridcell into a new
    # column, by default called 'data'.
    group_by(lon, lat) |>
    tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    mutate(data = purrr::map(data, ~my_pcwd(.)))


  # write (complemented) data to cwd-file with meaningful name and index counter
  path_cwd <- paste0(outdir_cwd, "/", fileprefix_cwd, "_", ilon, ".rds")
  message(
    paste0(
      "Writing file ", path_cwd , " ..."
    )
  )
  #' @importFrom readr write_rds
  readr::write_rds(
    out_cwd,
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
    out_pcwd,
    path_pcwd
  )

  # don't return data - it's written to file
}
